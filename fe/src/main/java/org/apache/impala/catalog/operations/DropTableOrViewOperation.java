package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.TransactionKeepalive.HeartbeatContext;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.util.AcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropTableOrViewOperation extends CatalogOperation {

  private static final Logger LOG = LoggerFactory
      .getLogger(DropTableOrViewOperation.class);
  private final TDropTableOrViewParams params;
  private boolean skipHMSOperation;
  private boolean skipCatalogOperation;
  private Table tbl;

  public DropTableOrViewOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response,
      CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
    params = Preconditions.checkNotNull(ddlExecRequest.drop_table_or_view_params);
  }

  @Override
  protected boolean takeDdlLock() {
    return true;
  }

  @Override
  protected void doHmsOperations() throws ImpalaException {

  }

  @Override
  protected void doCatalogOperations() throws ImpalaException {

  }

  @Override
  protected void before() throws ImpalaException {
    TableName tableName = TableName.fromThrift(params.getTable_name());
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
    long lockId = -1;
    if (tbl != null && !(tbl instanceof IncompleteTable) &&
        AcidUtils.isTransactionalTable(tbl.getMetaStoreTable().getParameters())) {
      HeartbeatContext ctx = new HeartbeatContext(
          String.format("Drop table/view %s.%s", tableName.getDb(), tableName.getTbl()),
          System.nanoTime());
      lockId = catalog_.lockTableStandalone(tableName.getDb(), tableName.getTbl(), ctx);
    }

  }

  @Override
  protected void after() {

  }
}
