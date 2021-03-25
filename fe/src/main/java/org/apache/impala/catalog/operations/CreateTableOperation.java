package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.IcebergCatalogOpExecutor;
import org.apache.impala.service.KuduCatalogOpExecutor;
import org.apache.impala.thrift.JniCatalogConstants;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.THdfsCachingOp;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.IcebergUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a new table in the metastore and adds an entry to the metadata cache to
 * lazily load the new metadata on the next access. If this is a Synchronized Kudu
 * table, the table is also created in the Kudu storage engine. Re-throws any HMS or
 * Kudu exceptions encountered during the create.
 * @param  syncDdl tells if SYNC_DDL option is enabled on this DDL request.
 * @return true if a new table has been created with the given params, false
 * otherwise.
 */
public class CreateTableOperation extends CatalogDdlOperation {

  private static final Logger LOG = LoggerFactory.getLogger(CreateTableOperation.class);
  // protected since this is already needed by CreateTableLikeOperation
  // the before methods sets this field to a metastore table object from the params
  protected org.apache.hadoop.hive.metastore.api.Table newTable;
  // if the table is set to cached, this field keeps track of the directive id
  private Long cacheDirId;
  private TCreateTableParams params;
  protected boolean skipHMSOperation;
  protected Table existingTable;
  private boolean takeDdlLock;
  protected List<SQLPrimaryKey> primaryKeys;
  protected List<SQLForeignKey> foreignKeys;
  protected TableName tableName;
  protected boolean ifNotExists;

  public CreateTableOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response, CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
  }

  private void alreadyExistsSummary(org.apache.hadoop.hive.metastore.api.Table newTable) {
    if ("VIEW".equalsIgnoreCase(newTable.getTableType())) {
      addSummary(response, "View already exists.");
    } else {
      addSummary(response, "Table already exists.");
    }
  }

  private void hasBeenCreatedSummary(
      org.apache.hadoop.hive.metastore.api.Table newTable) {
    if ("VIEW".equalsIgnoreCase(newTable.getTableType())) {
      addSummary(response, "View has been created.");
    } else {
      addSummary(response, "Table has been created.");
    }
  }

  @Override
  protected boolean requiresDdlLock() {
    return takeDdlLock;
  }

  @Override
  protected void doHmsOperations() throws ImpalaException {
    LOG.trace("Creating table {}", tableName);
    if (skipHMSOperation) {
      alreadyExistsSummary(newTable);
      LOG.trace(String.format("Skipping table creation because %s already exists and " +
          "IF NOT EXISTS was specified.", tableName));
      return;
    }
    LOG.trace("Creating table {}", tableName);
    if (KuduTable.isKuduTable(newTable)) {
      response.setNew_table_created(createKuduTable(newTable, params, response));
      return;
    } else if (IcebergTable.isIcebergTable(newTable)) {
      response.setNew_table_created(createIcebergTable(newTable, params, response));
      return;
    }
    Preconditions.checkState(params.getColumns().size() > 0,
        "Empty column list given as argument to Catalog.createTable");

    THdfsCachingOp cacheOp = params.cache_op;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      if (primaryKeys == null && foreignKeys == null) {
        msClient.getHiveClient().createTable(newTable);
      } else {
        MetastoreShim.createTableWithConstraints(
            msClient.getHiveClient(), newTable,
            primaryKeys == null ? new ArrayList<>() : primaryKeys,
            foreignKeys == null ? new ArrayList<>() : foreignKeys);
      }
      // TODO (HIVE-21807): Creating a table and retrieving the table information is
      // not atomic.
      response.setNew_table_created(true);
      hasBeenCreatedSummary(newTable);
      org.apache.hadoop.hive.metastore.api.Table msTable = msClient.getHiveClient()
          .getTable(newTable.getDbName(), newTable.getTableName());
      long tableCreateTime = msTable.getCreateTime();
      response.setTable_name(newTable.getDbName() + "." + newTable.getTableName());
      response.setTable_create_time(tableCreateTime);
      // For external tables set table location needed for lineage generation.
      if (newTable.getTableType() == TableType.EXTERNAL_TABLE.toString()) {
        String tableLocation = newTable.getSd().getLocation();
        // If location was not specified in the query, get it from newly created
        // metastore table.
        if (tableLocation == null) {
          tableLocation = msTable.getSd().getLocation();
        }
        response.setTable_location(tableLocation);
      }
      // If this table should be cached, and the table location was not specified by
      // the user, an extra step is needed to read the table to find the location.
      if (cacheOp != null && cacheOp.isSet_cached() &&
          newTable.getSd().getLocation() == null) {
        newTable = msClient.getHiveClient().getTable(
            newTable.getDbName(), newTable.getTableName());
      }
    } catch (Exception e) {
      if (e instanceof AlreadyExistsException && ifNotExists) {
        alreadyExistsSummary(newTable);
      }
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "createTable"), e);
    }
    // Submit the cache request and update the table metadata.
    if (cacheOp != null && cacheOp.isSet_cached()) {
      short replication = cacheOp.isSetReplication() ? cacheOp.getReplication() :
          JniCatalogConstants.HDFS_DEFAULT_CACHE_REPLICATION_FACTOR;
      cacheDirId = HdfsCachingUtil.submitCacheTblDirective(newTable,
          cacheOp.getCache_pool_name(), replication);
      applyAlterTable(newTable);
    }
  }


  /**
   * Creates a new Kudu table. It should be noted that since HIVE-22158, HMS transforms a
   * create managed Kudu table request to a create external Kudu table with
   * <code>external.table.purge</code> property set to true. Such transformed Kudu
   * tables should be treated as managed (synchronized) tables to keep the user facing
   * behavior consistent.
   * <p>
   * For synchronized tables (managed or external tables with external.table.purge=true in
   * tblproperties): 1. If Kudu's integration with the Hive Metastore is not enabled, the
   * Kudu table is first created in Kudu, then in the HMS. 2. Otherwise, when the table is
   * created in Kudu, we rely on Kudu to have created the table in the HMS. For external
   * tables: 1. We only create the table in the HMS (regardless of Kudu's integration with
   * the Hive Metastore).
   * <p>
   * After the above is complete, we create the table in the catalog cache.
   * <p>
   * 'response' is populated with the results of this operation. Returns true if a new
   * table was created as part of this call, false otherwise.
   */
  private boolean createKuduTable(org.apache.hadoop.hive.metastore.api.Table newTable,
      TCreateTableParams params, TDdlExecResponse response)
      throws ImpalaException {
    Preconditions.checkState(KuduTable.isKuduTable(newTable));
    boolean createHMSTable;
    if (!KuduTable.isSynchronizedTable(newTable)) {
      // if this is not a synchronized table, we assume that the table must be existing
      // in kudu and use the column spec from Kudu
      KuduCatalogOpExecutor.populateExternalTableColsFromKudu(newTable);
      createHMSTable = true;
    } else {
      // if this is a synchronized table (managed or external.purge table) then we
      // create it in Kudu first
      KuduCatalogOpExecutor.createSynchronizedTable(newTable, params);
      createHMSTable = !isKuduHmsIntegrationEnabled(newTable);
    }

    try {
      // Add the table to the HMS and the catalog cache. Acquire metastoreDdlLock_ to
      // ensure the atomicity of these operations.
      if (createHMSTable) {
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
          boolean tableInMetastore =
              msClient.getHiveClient().tableExists(newTable.getDbName(),
                  newTable.getTableName());
          if (!tableInMetastore) {
            msClient.getHiveClient().createTable(newTable);
          } else {
            alreadyExistsSummary(newTable);
            return false;
          }
        }
      }
    } catch (Exception e) {
      try {
        // Error creating the table in HMS, drop the synchronized table from Kudu.
        if (!KuduTable.isSynchronizedTable(newTable)) {
          KuduCatalogOpExecutor.dropTable(newTable, false);
        }
      } catch (Exception logged) {
        String kuduTableName = newTable.getParameters().get(KuduTable.KEY_TABLE_NAME);
        LOG.error(String.format("Failed to drop Kudu table '%s'", kuduTableName),
            logged);
        throw new RuntimeException(String.format("Failed to create the table '%s' in " +
                " the Metastore and the newly created Kudu table '%s' could not be " +
                " dropped. The log contains more information.", newTable.getTableName(),
            kuduTableName), e);
      }
      if (e instanceof AlreadyExistsException && params.if_not_exists) {
        alreadyExistsSummary(newTable);
        return false;
      }
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "createTable"), e);
    }
    hasBeenCreatedSummary(newTable);
    return true;
  }

  /**
   * Creates a new Iceberg table.
   */
  private boolean createIcebergTable(org.apache.hadoop.hive.metastore.api.Table newTable,
      TCreateTableParams params, TDdlExecResponse response)
      throws ImpalaException {
    Preconditions.checkState(IcebergTable.isIcebergTable(newTable));

    try {
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        boolean tableInMetastore =
            msClient.getHiveClient().tableExists(newTable.getDbName(),
                newTable.getTableName());
        if (!tableInMetastore) {
          TIcebergCatalog catalog = IcebergUtil.getTIcebergCatalog(newTable);
          String location = newTable.getSd().getLocation();
          //Create table in iceberg if necessary
          if (IcebergTable.isSynchronizedTable(newTable)) {
            //Set location here if not been specified in sql
            if (location == null) {
              if (catalog == TIcebergCatalog.HADOOP_CATALOG) {
                // Using catalog location to create table
                // We cannot set location for 'hadoop.catalog' table in SQL
                location = IcebergUtil.getIcebergCatalogLocation(newTable);
              } else {
                // Using normal location as 'hadoop.tables' table location and create
                // table
                location = MetastoreShim.getPathForNewTable(
                    msClient.getHiveClient().getDatabase(newTable.getDbName()),
                    newTable);
              }
            }
            String tableLoc = IcebergCatalogOpExecutor.createTable(catalog,
                IcebergUtil.getIcebergTableIdentifier(newTable), location, params)
                .location();
            newTable.getSd().setLocation(tableLoc);
          } else {
            if (location == null) {
              if (catalog == TIcebergCatalog.HADOOP_CATALOG) {
                // When creating external Iceberg table with 'hadoop.catalog' we load
                // the Iceberg table using catalog location and table identifier to get
                // the actual location of the table. This way we can also get the
                // correct location for tables stored in nested namespaces.
                TableIdentifier identifier =
                    IcebergUtil.getIcebergTableIdentifier(newTable);
                newTable.getSd().setLocation(IcebergUtil.loadTable(
                    TIcebergCatalog.HADOOP_CATALOG, identifier,
                    IcebergUtil.getIcebergCatalogLocation(newTable)).location());
              } else {
                addSummary(response,
                    "Location is necessary for external iceberg table.");
                return false;
              }
            }
          }

          // Iceberg tables are always unpartitioned. The partition columns are
          // derived from the TCreateTableParams.partition_spec field, and could
          // include one or more of the table columns
          Preconditions.checkState(newTable.getPartitionKeys() == null ||
              newTable.getPartitionKeys().isEmpty());
          if (!isIcebergHmsIntegrationEnabled(newTable)) {
            msClient.getHiveClient().createTable(newTable);
          } else {
            // Currently HiveCatalog doesn't set the table property
            // 'external.table.purge' during createTable().
            org.apache.hadoop.hive.metastore.api.Table msTbl =
                msClient.getHiveClient().getTable(
                    newTable.getDbName(), newTable.getTableName());
            msTbl.putToParameters("external.table.purge", "TRUE");
            // HiveCatalog also doesn't set the table properties either.
            for (Map.Entry<String, String> entry :
                params.getTable_properties().entrySet()) {
              msTbl.putToParameters(entry.getKey(), entry.getValue());
            }
            msClient.getHiveClient().alter_table(
                newTable.getDbName(), newTable.getTableName(), msTbl);
          }
        } else {
          alreadyExistsSummary(newTable);
          return false;
        }
      }
    } catch (Exception e) {
      if (e instanceof AlreadyExistsException && params.if_not_exists) {
        alreadyExistsSummary(newTable);
        return false;
      }
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "createTable"), e);
    }

    hasBeenCreatedSummary(newTable);
    return true;
  }

  @Override
  protected void doCatalogOperations() throws ImpalaException {
    if (syncDdl && existingTable != null) {
      // When SYNC_DDL is enabled and the table already exists, we force a version
      // bump on it so that it is added to the next statestore update. Without this
      // we could potentially be referring to a table object that has already been
      // GC'ed from the TopicUpdateLog and waitForSyncDdlVersion() cannot find a
      // covering topic version (IMPALA-7961).
      //
      // This is a conservative hack to not break the SYNC_DDL semantics and could
      // possibly result in false-positive invalidates on this table. However, that is
      // better than breaking the SYNC_DDL semantics and the subsequent queries
      // referring to this table failing with "table not found" errors.
      long newVersion = catalog_.incrementAndGetCatalogVersion();
      existingTable.setCatalogVersion(newVersion);
      LOG.trace("Table {} version bumped to {} because SYNC_DDL is enabled.",
          existingTable.getFullName(), newVersion);
      addTableToCatalogUpdate(existingTable, wantMinimalResult, response.result);
      return;
    }
    Preconditions.checkState(newTable != null);
    if (cacheDirId != null) {
      catalog_.watchCacheDirs(Lists.<Long>newArrayList(cacheDirId),
          new TTableName(newTable.getDbName(), newTable.getTableName()),
          "CREATE TABLE CACHED");
    }
    // Add the table to the catalog cache
    Table newTbl = catalog_.addIncompleteTable(newTable.getDbName(),
        newTable.getTableName());
    addTableToCatalogUpdate(newTbl, wantMinimalResult, response.result);
    if (catalogOpExecutor_.getAuthzConfig().isEnabled()) {
      catalogOpExecutor_.getAuthzManager()
          .updateTableOwnerPrivilege(params.server_name, newTable.getDbName(),
              newTable.getTableName(), /* oldOwner */ null,
              /* oldOwnerType */ null, newTable.getOwner(), newTable.getOwnerType(),
              response);
    }
  }

  @Override
  protected void init() throws ImpalaException {
    params = request.getCreate_table_params();
    Preconditions.checkNotNull(params);
    tableName = TableName.fromThrift(params.getTable_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null,
        "Null column list given as argument to Catalog.createTable");
    Preconditions.checkState(!catalog_.isBlacklistedTable(tableName),
        String.format("Can't create blacklisted table: %s. %s", tableName,
            BLACKLISTED_TABLES_INCONSISTENT_ERR_STR));
    primaryKeys = params.primary_keys;
    foreignKeys = params.foreign_keys;
    ifNotExists = params.if_not_exists;
    existingTable = catalog_.getTableNoThrow(tableName.getDb(), tableName.getTbl());
    if (params.if_not_exists && existingTable != null) {
      LOG.trace("Skipping table creation because {} already exists and " +
          "IF NOT EXISTS was specified.", tableName);
      catalogOpExecutor_.tryWriteLock(existingTable);
      skipHMSOperation = true;
    } else {
      // we are going to create a new table; take ddl lock
      takeDdlLock = true;
    }
    newTable = createMetaStoreTable(params);
  }

  @Override
  protected void cleanUp() {
    if (params.if_not_exists && existingTable != null) {
      // Release the locks held in tryLock().
      catalog_.getLock().writeLock().unlock();
      existingTable.releaseWriteLock();
    }
  }
}
