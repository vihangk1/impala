package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.View;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TCreateOrAlterViewParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlterViewOperation extends CatalogOperation {

  private static final Logger LOG = LoggerFactory.getLogger(AlterViewOperation.class);
  private long newCatalogVersion;
  private Table tbl;
  private TCreateOrAlterViewParams params;
  // alter HMS table object
  private org.apache.hadoop.hive.metastore.api.Table msTbl;

  public AlterViewOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response, CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
  }

  @Override
  protected boolean takeDdlLock() {
    return false;
  }

  @Override
  public void doHmsOperations() throws ImpalaException {
    addCatalogServiceIdentifiers(catalog_, tbl, newCatalogVersion);
    TableName tableName = TableName.fromThrift(params.getView_name());
    // Operate on a copy of the metastore table to avoid prematurely applying the
    // alteration to our cached table in case the actual alteration fails.
    msTbl =
        tbl.getMetaStoreTable().deepCopy();
    if (!msTbl.getTableType().equalsIgnoreCase(
        (TableType.VIRTUAL_VIEW.toString()))) {
      throw new ImpalaRuntimeException(
          String.format("ALTER VIEW not allowed on a table: %s",
              tableName.toString()));
    }

    // Set the altered view attributes and update the metastore.
    setAlterViewAttributes(params, msTbl);
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Altering view %s", tableName));
    }
    applyAlterTable(msTbl);
  }

  @Override
  public void doCatalogOperations() throws ImpalaException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      tbl.load(true, msClient.getHiveClient(), msTbl, "ALTER VIEW");
    }
    addSummary(response, "View has been altered.");
    tbl.setCatalogVersion(newCatalogVersion);
    addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
  }

  @Override
  public void before() throws ImpalaException {
    params = Preconditions.checkNotNull(request.getAlter_view_params());
    TableName tableName = TableName.fromThrift(params.getView_name());
    Preconditions.checkState(tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
            params.getColumns().size() > 0,
        "Null or empty column list given as argument to DdlExecutor.alterView");
    tbl = catalogOpExecutor_.getExistingTable(tableName.getDb(), tableName.getTbl(),
        "Load for ALTER VIEW");
    Preconditions.checkState(tbl instanceof View, "Expected view: %s",
        tableName);
    catalogOpExecutor_.tryWriteLock(tbl);
    newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
  }

  @Override
  public void after() {
    catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
    tbl.releaseWriteLock();
  }

  /**
   * Sets the given params in the metastore table as appropriate for an alter view
   * operation.
   */
  private void setAlterViewAttributes(TCreateOrAlterViewParams params,
      org.apache.hadoop.hive.metastore.api.Table view) {
    view.setViewOriginalText(params.getOriginal_view_def());
    view.setViewExpandedText(params.getExpanded_view_def());
    if (params.isSetComment() && params.getComment() != null) {
      view.getParameters().put("comment", params.getComment());
    }
    // Add all the columns to a new storage descriptor.
    view.getSd().setCols(buildFieldSchemaList(params.getColumns()));
  }
}
