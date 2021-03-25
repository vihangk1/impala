package org.apache.impala.catalog.operations;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.impala.analysis.AlterTableSortByStmt;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.View;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TCreateTableLikeParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.HdfsCachingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a new table in the metastore based on the definition of an existing table.
 * No data is copied as part of this process, it is a metadata only operation. If the
 * creation succeeds, an entry is added to the metadata cache to lazily load the new
 * table's metadata on the next access.
 * @param  syncDdl tells is SYNC_DDL is enabled for this DDL request.
 */

public class CreateTableLikeOperation extends CreateTableOperation {

  private static final Logger LOG = LoggerFactory
      .getLogger(CreateTableLikeOperation.class);
  private TCreateTableLikeParams createTableLikeParams;

  public CreateTableLikeOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response,
      CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
  }

  @Override
  public void init() throws ImpalaException {
    createTableLikeParams = Preconditions.checkNotNull(request.getCreate_table_like_params());
    tableName = TableName.fromThrift(createTableLikeParams.getTable_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    ifNotExists = createTableLikeParams.if_not_exists;

    THdfsFileFormat fileFormat =
        createTableLikeParams.isSetFile_format() ? createTableLikeParams.getFile_format() : null;
    String comment = createTableLikeParams.isSetComment() ? createTableLikeParams.getComment() : null;
    TableName tblName = TableName.fromThrift(createTableLikeParams.getTable_name());
    TableName srcTblName = TableName.fromThrift(createTableLikeParams.getSrc_table_name());
    Preconditions.checkState(tblName != null && tblName.isFullyQualified());
    Preconditions.checkState(srcTblName != null && srcTblName.isFullyQualified());
    Preconditions.checkState(!catalog_.isBlacklistedTable(tblName),
        String.format("Can't create blacklisted table: %s. %s", tblName,
            CatalogDdlOperation.BLACKLISTED_TABLES_INCONSISTENT_ERR_STR));

    Table srcTable = catalogOpExecutor_.getExistingTable(srcTblName.getDb(), srcTblName.getTbl(),
        "Load source for CREATE TABLE LIKE");
    org.apache.hadoop.hive.metastore.api.Table tbl =
        srcTable.getMetaStoreTable().deepCopy();
    Preconditions.checkState(!KuduTable.isKuduTable(tbl),
        "CREATE TABLE LIKE is not supported for Kudu tables.");
    tbl.setDbName(tblName.getDb());
    tbl.setTableName(tblName.getTbl());
    tbl.setOwner(createTableLikeParams.getOwner());
    if (tbl.getParameters() == null) {
      tbl.setParameters(new HashMap<String, String>());
    }
    if (createTableLikeParams.isSetSort_columns() && !createTableLikeParams.sort_columns.isEmpty()) {
      tbl.getParameters().put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS,
          Joiner.on(",").join(createTableLikeParams.sort_columns));
      TSortingOrder sortingOrder = createTableLikeParams.isSetSorting_order() ?
          createTableLikeParams.sorting_order : TSortingOrder.LEXICAL;
      tbl.getParameters().put(AlterTableSortByStmt.TBL_PROP_SORT_ORDER,
          sortingOrder.toString());
    }
    if (comment != null) {
      tbl.getParameters().put("comment", comment);
    }
    // The EXTERNAL table property should not be copied from the old table.
    if (createTableLikeParams.is_external) {
      tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      tbl.putToParameters("EXTERNAL", "TRUE");
    } else {
      tbl.setTableType(TableType.MANAGED_TABLE.toString());
      if (tbl.getParameters().containsKey("EXTERNAL")) {
        tbl.getParameters().remove("EXTERNAL");
      }
    }

    // We should not propagate hdfs caching parameters to the new table.
    if (tbl.getParameters().containsKey(
        HdfsCachingUtil.CACHE_DIR_ID_PROP_NAME)) {
      tbl.getParameters().remove(HdfsCachingUtil.CACHE_DIR_ID_PROP_NAME);
    }
    if (tbl.getParameters().containsKey(
        HdfsCachingUtil.CACHE_DIR_REPLICATION_PROP_NAME)) {
      tbl.getParameters().remove(
          HdfsCachingUtil.CACHE_DIR_REPLICATION_PROP_NAME);
    }

    // The LOCATION property should not be copied from the old table. If the location
    // is null (the caller didn't specify a custom location) this will clear the value
    // and the table will use the default table location from the parent database.
    tbl.getSd().setLocation(createTableLikeParams.getLocation());
    if (fileFormat != null) {
      setStorageDescriptorFileFormat(tbl.getSd(), fileFormat);
    } else if (srcTable instanceof View) {
      // Here, source table is a view which has no input format. So to be
      // consistent with CREATE TABLE, default input format is assumed to be
      // TEXT unless otherwise specified.
      setStorageDescriptorFileFormat(tbl.getSd(), THdfsFileFormat.TEXT);
    }
    // Set the row count of this table to unknown.
    tbl.putToParameters(StatsSetupConst.ROW_COUNT, "-1");
    setDefaultTableCapabilities(tbl);
    LOG.trace(String.format("Creating table %s LIKE %s", tblName, srcTblName));
    // set the newTable from the base class so that we can create it
    newTable = tbl;
  }

  @Override
  protected void cleanUp() {
    if (!ifNotExists && existingTable != null) {
      // Release the locks held in tryLock().
      catalog_.getLock().writeLock().unlock();
      existingTable.releaseWriteLock();
    }
  }
}
