package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.View;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.TransactionKeepalive.HeartbeatContext;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.IcebergCatalogOpExecutor;
import org.apache.impala.service.KuduCatalogOpExecutor;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.TTable;
import org.apache.impala.util.AcidUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drops a table or view from the metastore and removes it from the catalog.
 * Also drops all associated caching requests on the table and/or table's partitions,
 * uncaching all table data. If params.purge is true, table data is permanently
 * deleted.
 * In case of transactional tables acquires an exclusive HMS table lock before
 * executing the drop operation.
 */
public class DropTableOrViewOperation extends CatalogDdlOperation {

  private static final Logger LOG = LoggerFactory
      .getLogger(DropTableOrViewOperation.class);
  private final TDropTableOrViewParams params;
  private boolean skipHMSOperation;
  private boolean skipCatalogOperation;
  private Table tbl;
  private boolean takeDdlLock;
  private long lockId;
  private TableName tableName;
  private TCatalogObject removedObject = new TCatalogObject();

  public DropTableOrViewOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response,
      CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
    params = Preconditions.checkNotNull(ddlExecRequest.drop_table_or_view_params);
  }

  @Override
  protected boolean requiresDdlLock() {
    return takeDdlLock;
  }

  @Override
  protected void doHmsOperations() throws ImpalaException {
    // we are doing some catalog operations here strictly to check if db or
    // table exists or not. Ideally this should have been part of before() method,
    // but we do this here since we need to execute this code while holding the
    // metastore ddlLock.
    Db db = catalog_.getDb(params.getTable_name().db_name);
    if (db == null) {
      String dbNotExist = "Database does not exist: " + params.getTable_name().db_name;
      if (params.if_exists) {
        addSummary(response, dbNotExist);
        return;
      }
      throw new CatalogException(dbNotExist);
    }
    Table existingTbl = db.getTable(params.getTable_name().table_name);
    if (existingTbl == null) {
      if (params.if_exists) {
        addSummary(response, (params.is_table ? "Table " : "View ") + "does not exist.");
        return;
      }
      throw new CatalogException("Table/View does not exist.");
    }

    // Check to make sure we don't drop a view with "drop table" statement and
    // vice versa. is_table field is marked optional in TDropTableOrViewParams to
    // maintain catalog api compatibility.
    // TODO: Remove params.isSetIs_table() check once catalog api compatibility is
    // fixed.
    if (params.isSetIs_table() && ((params.is_table && existingTbl instanceof View)
        || (!params.is_table && !(existingTbl instanceof View)))) {
      String errorMsg = "DROP " + (params.is_table ? "TABLE " : "VIEW ") +
          "not allowed on a " + (params.is_table ? "view: " : "table: ") + tableName;
      if (params.if_exists) {
        addSummary(response, "Drop " + (params.is_table ? "table " : "view ") +
            "is not allowed on a " + (params.is_table ? "view." : "table."));
        return;
      }
      throw new CatalogException(errorMsg);
    }

    // Retrieve the HMS table to determine if this is a Kudu or Iceberg table.
    org.apache.hadoop.hive.metastore.api.Table msTbl = existingTbl.getMetaStoreTable();
    if (msTbl == null) {
      Preconditions.checkState(existingTbl instanceof IncompleteTable);
      Stopwatch hmsLoadSW = Stopwatch.createStarted();
      long hmsLoadTime;
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        msTbl = msClient.getHiveClient().getTable(tableName.getDb(),
            tableName.getTbl());
      } catch (TException e) {
        LOG.error(String.format(HMS_RPC_ERROR_FORMAT_STR, "getTable") + e.getMessage());
      } finally {
        hmsLoadTime = hmsLoadSW.elapsed(TimeUnit.NANOSECONDS);
      }
      existingTbl.updateHMSLoadTableSchemaTime(hmsLoadTime);
    }
    boolean isSynchronizedKuduTable = msTbl != null &&
        KuduTable.isKuduTable(msTbl) && KuduTable.isSynchronizedTable(msTbl);
    if (isSynchronizedKuduTable) {
      KuduCatalogOpExecutor.dropTable(msTbl, /* if exists */ true);
    }

    boolean isSynchronizedIcebergTable = msTbl != null &&
        IcebergTable.isIcebergTable(msTbl) &&
        IcebergTable.isSynchronizedTable(msTbl);
    if (!(existingTbl instanceof IncompleteTable) && isSynchronizedIcebergTable) {
      Preconditions.checkState(existingTbl instanceof IcebergTable);
      IcebergCatalogOpExecutor.dropTable((IcebergTable) existingTbl, params.if_exists);
    }

    // When HMS integration is automatic, the table is dropped automatically. In all
    // other cases, we need to drop the HMS table entry ourselves.
    boolean isSynchronizedTable = isSynchronizedKuduTable || isSynchronizedIcebergTable;
    boolean needsHmsDropTable =
        (existingTbl instanceof IncompleteTable && isSynchronizedIcebergTable) ||
            !isSynchronizedTable ||
            !isHmsIntegrationAutomatic(msTbl);
    if (needsHmsDropTable) {
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        msClient.getHiveClient().dropTable(
            tableName.getDb(), tableName.getTbl(), true,
            params.if_exists, params.purge);
      } catch (NoSuchObjectException e) {
        throw new ImpalaRuntimeException(String.format("Table %s no longer exists " +
            "in the Hive MetaStore. Run 'invalidate metadata %s' to update the " +
            "Impala catalog.", tableName, tableName));
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "dropTable"), e);
      }
    }
    addSummary(response, (params.is_table ? "Table " : "View ") + "has been dropped.");
  }

  @Override
  protected void doCatalogOperations() throws ImpalaException {
    Table table = catalog_.removeTable(params.getTable_name().db_name,
        params.getTable_name().table_name);
    if (table == null) {
      // Nothing was removed from the catalogd's cache.
      response.result.setVersion(catalog_.getCatalogVersion());
      return;
    }
    response.result.setVersion(table.getCatalogVersion());
    uncacheTable(table);
    if (table.getMetaStoreTable() != null) {
      if (catalogOpExecutor_.getAuthzConfig().isEnabled()) {
        catalogOpExecutor_.getAuthzManager().updateTableOwnerPrivilege(params.server_name,
            table.getDb().getName(), table.getName(),
            table.getMetaStoreTable().getOwner(),
            table.getMetaStoreTable().getOwnerType(), /* newOwner */ null,
            /* newOwnerType */ null, response);
      }
    }
  }

  @Override
  protected void init() throws ImpalaException {
    tableName = TableName.fromThrift(params.getTable_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(!catalog_.isBlacklistedTable(tableName) || params.if_exists,
        String.format("Can't drop blacklisted table: %s. %s", tableName,
            BLACKLISTED_TABLES_INCONSISTENT_ERR_STR));
    if (catalog_.isBlacklistedTable(tableName)) {
      // It's expected to go here if "if_exists" is set to true.
      addSummary(response, "Can't drop blacklisted table: " + tableName);
      skipCatalogOperation = true;
      skipHMSOperation = true;
      return;
    }
    LOG.trace(String.format("Dropping table/view %s", tableName));

    // If the table exists, ensure that it is loaded before we try to operate on it.
    // We do this up here rather than down below to avoid doing too much table-loading
    // work while holding the DDL lock. We can't simply use 'getExistingTable' because
    // we rely on more granular checks to provide the correct summary message for
    // the 'IF EXISTS' case.
    //
    // In the standard catalogd implementation, the table will most likely already
    // be loaded because the planning phase on the impalad side triggered the loading.
    // In the LocalCatalog configuration, however, this is often necessary.
    try {
      // we pass null validWriteIdList here since we don't really care what version of
      // table is loaded, eventually its going to be dropped below.
      catalog_.getOrLoadTable(params.getTable_name().db_name,
          params.getTable_name().table_name, "Load for DROP TABLE/VIEW", null);
    } catch (CatalogException e) {
      // Ignore exceptions -- the above was just to trigger loading. Failure to load
      // or non-existence of the database will be handled down below.
    }

    tbl = catalog_.getTableIfCachedNoThrow(tableName.getDb(), tableName.getTbl());
    lockId = -1;
    if (tbl != null && !(tbl instanceof IncompleteTable) &&
        AcidUtils.isTransactionalTable(tbl.getMetaStoreTable().getParameters())) {
      HeartbeatContext ctx = new HeartbeatContext(
          String.format("Drop table/view %s.%s", tableName.getDb(), tableName.getTbl()),
          System.nanoTime());
      lockId = catalog_.lockTableStandalone(tableName.getDb(), tableName.getTbl(), ctx);
    }
    takeDdlLock = true;
  }

  @Override
  protected void cleanUp() throws ImpalaException {
    removedObject.setType(TCatalogObjectType.TABLE);
    removedObject.setTable(new TTable());
    removedObject.getTable().setTbl_name(tableName.getTbl());
    removedObject.getTable().setDb_name(tableName.getDb());
    removedObject.setCatalog_version(response.result.getVersion());
    response.result.addToRemoved_catalog_objects(removedObject);
    if (lockId > 0) catalog_.releaseTableLock(lockId);
  }
}
