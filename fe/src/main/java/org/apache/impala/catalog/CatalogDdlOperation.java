package org.apache.impala.catalog;

import static org.apache.impala.service.CatalogOpExecutor.HMS_RPC_ERROR_FORMAT_STR;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.util.AcidUtils.TblTransaction;
import org.apache.thrift.TException;

public abstract class CatalogDdlOperation {

  protected final CatalogServiceCatalog catalog_;
  protected final TDdlExecRequest request_;
  protected final TDdlExecResponse response_;

  public CatalogDdlOperation(CatalogServiceCatalog catalog, TDdlExecRequest request,
      TDdlExecResponse response) {
    catalog_ = Preconditions.checkNotNull(catalog);
    request_ = Preconditions.checkNotNull(request);
    response_ = Preconditions.checkNotNull(response);

  }

  protected abstract void execute() throws CatalogException, ImpalaRuntimeException;

  /**
   * Create result set from string 'summary', and attach it to 'response'.
   */
  public static void addSummary(TDdlExecResponse response, String summary) {
    TColumnValue resultColVal = new TColumnValue();
    resultColVal.setString_val(summary);
    TResultSet resultSet = new TResultSet();
    resultSet.setSchema(new TResultSetMetadata(Lists.newArrayList(new TColumn(
        "summary", Type.STRING.toThrift()))));
    TResultRow resultRow = new TResultRow();
    resultRow.setColVals(Lists.newArrayList(resultColVal));
    resultSet.setRows(Lists.newArrayList(resultRow));
    response.setResult_set(resultSet);
  }

  /**
   * Conveniance function to call applyAlterTable(3) with default arguments.
   */
  protected void applyAlterTable(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws ImpalaRuntimeException {
    applyAlterTable(msTbl, true, null);
  }

  /**
   * Applies an ALTER TABLE command to the metastore table. Note: The metastore interface
   * is not very safe because it only accepts an entire metastore.api.Table object rather
   * than a delta of what to change. This means an external modification to the table
   * could be overwritten by an ALTER TABLE command if the metadata is not completely
   * in-sync. This affects both Hive and Impala, but is more important in Impala because
   * the metadata is cached for a longer period of time. If 'overwriteLastDdlTime' is
   * true, then table property 'transient_lastDdlTime' is updated to current time so that
   * metastore does not update it in the alter_table call.
   */
  protected void applyAlterTable(org.apache.hadoop.hive.metastore.api.Table msTbl,
      boolean overwriteLastDdlTime, TblTransaction tblTxn)
      throws ImpalaRuntimeException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      if (overwriteLastDdlTime) {
        // It would be enough to remove this table property, as HMS would fill it, but
        // this would make it necessary to reload the table after alter_table in order to
        // remain consistent with HMS.
        Table.updateTimestampProperty(msTbl, Table.TBL_PROP_LAST_DDL_TIME);
      }

      // Avoid computing/setting stats on the HMS side because that may reset the
      // 'numRows' table property (see HIVE-15653). The DO_NOT_UPDATE_STATS flag
      // tells the HMS not to recompute/reset any statistics on its own. Any
      // stats-related alterations passed in the RPC will still be applied.
      msTbl.putToParameters(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);

      if (tblTxn != null) {
        MetastoreShim.alterTableWithTransaction(msClient.getHiveClient(), msTbl, tblTxn);
      } else {
        try {
          msClient.getHiveClient().alter_table(
              msTbl.getDbName(), msTbl.getTableName(), msTbl);
        } catch (TException e) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_table"), e);
        }
      }
    }
  }
}
