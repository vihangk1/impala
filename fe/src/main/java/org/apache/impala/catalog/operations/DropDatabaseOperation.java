package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.KuduCatalogOpExecutor;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Drops a database from the metastore and removes the database's metadata from the
 * internal cache. Attempts to remove the HDFS cache directives of the underlying
 * tables. Re-throws any HMS exceptions encountered during the drop.
 */
public class DropDatabaseOperation extends CatalogOperation {

  private static final Logger LOG = LoggerFactory.getLogger(DropDatabaseOperation.class);
  private final TDropDbParams params;
  private boolean skipHmsOperation;
  private boolean skipCatalogOperation;
  private Db db;
  private String dbName;
  private TCatalogObject removedObject;

  public DropDatabaseOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response,
      CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
    params = Preconditions.checkNotNull(ddlExecRequest.drop_db_params);
  }

  @Override
  protected boolean takeDdlLock() {
    return true;
  }

  @Override
  protected void doHmsOperations() throws ImpalaException {
    if (skipHmsOperation) {
      return;
    }
    // Remove all the Kudu tables of 'db' from the Kudu storage engine.
    if (db != null && params.cascade) dropTablesFromKudu(db);
    // The Kudu tables in the HMS should have been dropped at this point
    // with the Hive Metastore integration enabled.
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      // HMS client does not have a way to identify if the database was dropped or
      // not if the ignoreIfUnknown flag is true. Hence we always pass the
      // ignoreIfUnknown as false and catch the NoSuchObjectFoundException and
      // determine if we should throw or not
      msClient.getHiveClient().dropDatabase(
          dbName, /* deleteData */true, /* ignoreIfUnknown */false,
          params.cascade);
      addSummary(response, "Database has been dropped.");
    } catch (TException e) {
      if (e instanceof NoSuchObjectException && params.if_exists) {
        // if_exists param was set; we ignore the NoSuchObjectFoundException
        addSummary(response, "Database does not exist.");
      } else {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "dropDatabase"), e);
      }
    }
  }

  /**
   * Drops all the Kudu tables of database 'db' from the Kudu storage engine. Retrieves
   * the Kudu table name of each table in 'db' from HMS. Throws an ImpalaException if
   * metadata for Kudu tables cannot be loaded from HMS or if an error occurs while
   * trying to drop a table from Kudu.
   */
  private void dropTablesFromKudu(Db db) throws ImpalaException {
    // If the table format isn't available, because the table hasn't been loaded yet,
    // the metadata must be fetched from the Hive Metastore.
    List<String> incompleteTableNames = Lists.newArrayList();
    List<org.apache.hadoop.hive.metastore.api.Table> msTables = Lists.newArrayList();
    for (Table table: db.getTables()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = table.getMetaStoreTable();
      if (msTable == null) {
        incompleteTableNames.add(table.getName());
      } else {
        msTables.add(msTable);
      }
    }
    if (!incompleteTableNames.isEmpty()) {
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        msTables.addAll(msClient.getHiveClient().getTableObjectsByName(
            db.getName(), incompleteTableNames));
      } catch (TException e) {
        LOG.error(String.format(HMS_RPC_ERROR_FORMAT_STR, "getTableObjectsByName") +
            e.getMessage());
      }
    }
    for (org.apache.hadoop.hive.metastore.api.Table msTable: msTables) {
      if (!KuduTable.isKuduTable(msTable) || !KuduTable
          .isSynchronizedTable(msTable)) continue;
      // The operation will be aborted if the Kudu table cannot be dropped. If for
      // some reason Kudu is permanently stuck in a non-functional state, the user is
      // expected to ALTER TABLE to either set the table to UNMANAGED or set the format
      // to something else.
      KuduCatalogOpExecutor.dropTable(msTable, /*if exists*/ true);
    }
  }

  @Override
  protected void doCatalogOperations() throws ImpalaException {
    if (skipCatalogOperation) {
      return;
    }
    Db removedDb = catalog_.removeDb(dbName);

    if (removedDb == null) {
      // Nothing was removed from the catalogd's cache.
      response.result.setVersion(catalog_.getCatalogVersion());
      return;
    }
    // Make sure the cache directives, if any, of the underlying tables are removed
    for (String tableName: removedDb.getAllTableNames()) {
      uncacheTable(removedDb.getTable(tableName));
    }
    removedObject = removedDb.toTCatalogObject();
    if (catalogOpExecutor_.getAuthzConfig().isEnabled()) {
      catalogOpExecutor_.getAuthzManager().updateDatabaseOwnerPrivilege(params.server_name, dbName,
          db.getMetaStoreDb().getOwnerName(), db.getMetaStoreDb().getOwnerType(),
          /* newOwner */ null, /* newOwnerType */ null, resp);
    }
  }


  /**
   * Drops all associated caching requests on the table and/or table's partitions,
   * uncaching all table data, if applicable. Throws no exceptions, only logs errors.
   * Does not update the HMS.
   */
  private static void uncacheTable(FeTable table) {
    if (!(table instanceof FeFsTable)) return;
    FeFsTable hdfsTable = (FeFsTable) table;
    if (hdfsTable.isMarkedCached()) {
      try {
        HdfsCachingUtil.removeTblCacheDirective(table.getMetaStoreTable());
      } catch (Exception e) {
        LOG.error("Unable to uncache table: " + table.getFullName(), e);
      }
    }
    if (table.getNumClusteringCols() > 0) {
      Collection<? extends FeFsPartition> parts =
          FeCatalogUtils.loadAllPartitions(hdfsTable);
      for (FeFsPartition part: parts) {
        if (part.isMarkedCached()) {
          HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(
              (HdfsPartition) part);
          try {
            HdfsCachingUtil.removePartitionCacheDirective(partBuilder);
            // We are dropping the table. Don't need to update the existing partition so
            // ignore the partBuilder here.
          } catch (Exception e) {
            LOG.error("Unable to uncache partition: " + part.getPartitionName(), e);
          }
        }
      }
    }
  }

  @Override
  protected void before() throws ImpalaException {
    dbName = params.getDb();
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name passed as argument to Catalog.dropDatabase");
    Preconditions.checkState(!catalog_.isBlacklistedDb(dbName) || params.if_exists,
        String.format("Can't drop blacklisted database: %s. %s", dbName,
            BLACKLISTED_DBS_INCONSISTENT_ERR_STR));
    if (catalog_.isBlacklistedDb(dbName)) {
      // It's expected to go here if "if_exists" is set to true.
      addSummary(response, "Can't drop blacklisted database: " + dbName);
      skipCatalogOperation = true;
      skipHmsOperation = true;
      return;
    }

    LOG.trace("Dropping database " + dbName);
    db = catalog_.getDb(dbName);
    if (db != null && db.numFunctions() > 0 && !params.cascade) {
      throw new CatalogException("Database " + db.getName() + " is not empty");
    }
  }

  @Override
  protected void after() {
    Preconditions.checkNotNull(removedObject);
    response.result.setVersion(removedObject.getCatalog_version());
    response.result.addToRemoved_catalog_objects(removedObject);
    // it is possible that HMS database has been removed out of band externally. In
    // such a case we still would want to add the summary of the operation as database
    // has been dropped since we cleaned up state from CatalogServer
    addSummary(response, "Database has been dropped.");
  }
}
