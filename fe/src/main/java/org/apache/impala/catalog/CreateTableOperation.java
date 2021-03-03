package org.apache.impala.catalog;

import static org.apache.impala.service.CatalogOpExecutor.HMS_RPC_ERROR_FORMAT_STR;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.JniCatalogConstants;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.THdfsCachingOp;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.HdfsCachingUtil;

public class CreateTableOperation extends CatalogDdlOperation {
  private Table msTblToBeCreated_;
  private org.apache.impala.catalog.Table newTbl_;
  public CreateTableOperation(CatalogServiceCatalog catalog,
      TDdlExecRequest request, TDdlExecResponse response, Table msTbl) {
    super(catalog, request, response);
    msTblToBeCreated_ = msTbl;
  }

  @Override
  public void execute() throws CatalogException, ImpalaRuntimeException {
    TCreateTableParams params = request_.create_table_params;
    boolean if_not_exists = params.if_not_exists;
    THdfsCachingOp cacheOp = params.getCache_op();
    List<SQLPrimaryKey> primaryKeys = params.getPrimary_keys();
    List<SQLForeignKey> foreignKeys = params.getForeign_keys();
    org.apache.hadoop.hive.metastore.api.Table msTable;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      if (primaryKeys == null && foreignKeys == null) {
        msClient.getHiveClient().createTable(msTblToBeCreated_);
      } else {
        MetastoreShim.createTableWithConstraints(
            msClient.getHiveClient(), msTblToBeCreated_,
            primaryKeys == null ? new ArrayList<>() : primaryKeys,
            foreignKeys == null ? new ArrayList<>() : foreignKeys);
      }
      // TODO (HIVE-21807): Creating a table and retrieving the table information is
      // not atomic.
      addSummary(response_, "Table has been created.");
      msTable = msClient.getHiveClient()
          .getTable(msTblToBeCreated_.getDbName(), msTblToBeCreated_.getTableName());
      long tableCreateTime = msTable.getCreateTime();
      response_.setTable_name(msTblToBeCreated_.getDbName() + "." + msTblToBeCreated_.getTableName());
      response_.setTable_create_time(tableCreateTime);
      // For external tables set table location needed for lineage generation.
      if (msTblToBeCreated_.getTableType() == TableType.EXTERNAL_TABLE.toString()) {
        String tableLocation = msTblToBeCreated_.getSd().getLocation();
        // If location was not specified in the query, get it from newly created
        // metastore table.
        if (tableLocation == null) {
          tableLocation = msTable.getSd().getLocation();
        }
        response_.setTable_location(tableLocation);
      }
      // If this table should be cached, and the table location was not specified by
      // the user, an extra step is needed to read the table to find the location.
      if (cacheOp != null && cacheOp.isSet_cached() &&
          msTblToBeCreated_.getSd().getLocation() == null) {
        msTblToBeCreated_ = msClient.getHiveClient().getTable(
            msTblToBeCreated_.getDbName(), msTblToBeCreated_.getTableName());
      }
    } catch (Exception e) {
      if (e instanceof AlreadyExistsException && if_not_exists) {
        addSummary(response_, "Table already exists");
        return;
      }
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "createTable"), e);
    }
    // Submit the cache request and update the table metadata.
    if (cacheOp != null && cacheOp.isSet_cached()) {
      short replication = cacheOp.isSetReplication() ? cacheOp.getReplication() :
          JniCatalogConstants.HDFS_DEFAULT_CACHE_REPLICATION_FACTOR;
      long id = HdfsCachingUtil.submitCacheTblDirective(msTblToBeCreated_,
          cacheOp.getCache_pool_name(), replication);
      catalog_.watchCacheDirs(Lists.<Long>newArrayList(id),
          new TTableName(msTblToBeCreated_.getDbName(), msTblToBeCreated_.getTableName()),
          "CREATE TABLE CACHED");
      applyAlterTable(msTblToBeCreated_);
    }
    // TODO(Self-event) update using events
    newTbl_ = catalog_
        .addIncompleteTable(msTblToBeCreated_.getDbName(),
            msTblToBeCreated_.getTableName(), msTable.getId());
  }

  public org.apache.impala.catalog.Table getCreatedTbl() {
    return newTbl_;
  }
}
