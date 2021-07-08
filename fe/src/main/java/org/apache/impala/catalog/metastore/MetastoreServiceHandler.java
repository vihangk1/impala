// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.catalog.metastore;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.AbstractThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.DefaultPartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.AlterPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AlterPartitionsResponse;
import org.apache.hadoop.hive.metastore.api.AlterTableRequest;
import org.apache.hadoop.hive.metastore.api.AlterTableResponse;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.RenamePartitionRequest;
import org.apache.hadoop.hive.metastore.api.RenamePartitionResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.impala.catalog.CatalogHmsAPIHelper;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.Reference;
import org.apache.impala.common.Pair;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.thrift.TTableName;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the HMS APIs that are redirected to the HMS server from CatalogD.
 * APIs that should be served from CatalogD must be overridden in {@link
 * CatalogMetastoreServer}.
 * <p>
 * Implementation Notes: Care should taken to use
 * {@link IMetaStoreClient#getThriftClient()}
 * method when forwarding a API call to HMS service since IMetastoreClient itself modifies
 * the arguments before sending the RPC to the HMS server. This can lead to unexpected
 * side-effects like (processorCapabilities do not match with the actual client).
 */
public abstract class MetastoreServiceHandler extends AbstractThriftHiveMetastore {

  private static final Logger LOG = LoggerFactory
      .getLogger(MetastoreServiceHandler.class);
  protected static final String METAEXCEPTION_MSG_FORMAT =
      "Unexpected error occurred while"
          + " executing %s. Cause: %s. See catalog logs for details.";
  protected static final String HMS_FALLBACK_MSG_FORMAT = "Forwarding the request %s for "
      + "table %s to the backing HiveMetastore service";

  // constant used for logging error messages
  protected final CatalogServiceCatalog catalog_;
  protected final boolean fallBackToHMSOnErrors_;
  // TODO handle session configuration
  protected Configuration serverConf_;
  protected PartitionExpressionProxy expressionProxy_;
  protected final String defaultCatalogName_;
  protected final boolean invalidateCacheOnDDLs_;

  public MetastoreServiceHandler(CatalogServiceCatalog catalog,
      boolean fallBackToHMSOnErrors) {
    catalog_ = Preconditions.checkNotNull(catalog);
    fallBackToHMSOnErrors_ = fallBackToHMSOnErrors;
    LOG.info("Fallback to hive metastore service on errors is {}",
        fallBackToHMSOnErrors_);
    // load the metastore configuration from the classpath
    serverConf_ = Preconditions.checkNotNull(MetastoreConf.newMetastoreConf());
    String className = MetastoreConf
        .get(serverConf_, ConfVars.EXPRESSION_PROXY_CLASS.getVarname());
    try {
      Preconditions.checkNotNull(className);
      LOG.info("Instantiating {}", className);
      expressionProxy_ = PartFilterExprUtil.createExpressionProxy(serverConf_);
      if (expressionProxy_ instanceof DefaultPartitionExpressionProxy) {
        LOG.error("PartFilterExprUtil.createExpressionProxy returned"
            + " DefaultPartitionExpressionProxy. Check if hive-exec"
            + " jar is available in the classpath.");
        expressionProxy_ = null;
      }
    } catch (Exception ex) {
      LOG.error("Could not instantiate {}", className, ex);
    }
    defaultCatalogName_ =
        MetaStoreUtils.getDefaultCatalog(serverConf_);
    //TODO: Instead of passing individual configs in MetastoreServiceHandler,
    //  we can either
    //  1. Create MetastoreServiceContext (which would have all the desired configs) and
    //     pass that in the constructor
    //                       OR
    //  2. Access config directly from BackendConfig INSTANCE directly.
    //  For now, going with option #2

    invalidateCacheOnDDLs_ =
        BackendConfig.INSTANCE.invalidateCatalogdHMSCacheOnDDLs();
    LOG.info("Invalidate catalogd cache for DDLs on non transactional tables " +
        "is set to {}",invalidateCacheOnDDLs_);
  }

  @Override
  @ServedFromCatalogd
  public void drop_table(String dbname, String tblname, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().drop_table(dbname, tblname, deleteData);
      removeNonTransactionalTableIfExists(dbname, tblname, "drop_table");
    }
  }

  @Override
  @ServedFromCatalogd
  public void drop_table_with_environment_context(String dbname, String tblname,
      boolean deleteData,
      EnvironmentContext environmentContext)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .drop_table_with_environment_context(dbname, tblname, deleteData,
              environmentContext);
      removeNonTransactionalTableIfExists(dbname, tblname,
          "drop_table_with_environment_context");
    }
  }

  /**
   * This method gets the table from the HMS directly. Additionally, if the request has
   * {@code getFileMetadata} set it computes the filemetadata and returns it in the
   * response. For transactional tables, it uses the ValidWriteIdList from the request and
   * gets the current ValidTxnList to get the requested snapshot of the file-metadata for
   * the table.
   */
  @Override
  @ServedFromCatalogd
  public GetTableResult get_table_req(GetTableRequest getTableRequest)
      throws MetaException, NoSuchObjectException, TException {
    String tblName = getTableRequest.getDbName() + "." + getTableRequest.getTblName();
    LOG.debug(String.format(HMS_FALLBACK_MSG_FORMAT, "get_table_req", tblName));
    GetTableResult result;
    ValidTxnList txnList = null;
    ValidWriteIdList writeIdList = null;
    String requestWriteIdList = getTableRequest.getValidWriteIdList();
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      result = client.getHiveClient().getThriftClient()
          .get_table_req(getTableRequest);
      Table tbl = result.getTable();
      // return early if file-metadata is not requested
      if (!getTableRequest.isGetFileMetadata()) {
        LOG.trace("File metadata is not requested. Returning table {}",
            tbl.getTableName());
        return result;
      }
      // we need to get the current ValidTxnIdList to avoid returning
      // file-metadata for in-progress compactions. If the request does not
      // include ValidWriteIdList or if the table is not transactional we compute
      // the file-metadata as seen on the file-system.
      boolean isTransactional = tbl.getParameters() != null && AcidUtils
          .isTransactionalTable(tbl.getParameters());
      if (isTransactional && requestWriteIdList != null) {
        txnList = MetastoreShim.getValidTxns(client.getHiveClient());
        writeIdList = MetastoreShim
            .getValidWriteIdListFromString(requestWriteIdList);
      }
    }
    CatalogHmsAPIHelper.loadAndSetFileMetadataFromFs(txnList, writeIdList, result);
    return result;
  }

  @Override
  @ServedFromCatalogd
  public void alter_table(String dbname, String tblName, Table newTable)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().alter_table(dbname, tblName, newTable);
      renameNonTransactionalTableIfExists(dbname, tblName, newTable.getDbName(),
          newTable.getTableName(),"alter_table");
    }
  }

  @Override
  @ServedFromCatalogd
  public void alter_table_with_environment_context(String dbname, String tblName,
      Table table,
      EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .alter_table_with_environment_context(dbname,
              tblName, table, environmentContext);
      renameNonTransactionalTableIfExists(dbname, tblName, table.getDbName(),
          table.getTableName(),"alter_table_with_environment_context");
    }
  }

  @Override
  @ServedFromCatalogd
  public void alter_table_with_cascade(String dbname, String tblName, Table table,
      boolean cascade)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().alter_table_with_cascade(dbname, tblName,
          table, cascade);
      renameNonTransactionalTableIfExists(dbname, tblName, table.getDbName(),
          table.getTableName(),"alter_table_with_cascade");
    }
  }

  @Override
  @ServedFromCatalogd
  public AlterTableResponse alter_table_req(AlterTableRequest alterTableRequest)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      AlterTableResponse response =
              client.getHiveClient().getThriftClient().alter_table_req(alterTableRequest);
      renameNonTransactionalTableIfExists(alterTableRequest.getDbName(),
          alterTableRequest.getTableName(), alterTableRequest.getTable().getDbName(),
          alterTableRequest.getTable().getTableName(),"alter_table_req");
      return response;
    }
  }

  @Override
  @ServedFromCatalogd
  public Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      Partition addedPartition =
          client.getHiveClient().getThriftClient().add_partition(partition);
      invalidateNonTransactionalTableIfExists(partition.getDbName(),
          partition.getTableName(), "add_partition");
      return addedPartition;
    }
  }

  @Override
  @ServedFromCatalogd
  public Partition add_partition_with_environment_context(Partition partition,
      EnvironmentContext environmentContext)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      Partition addedPartition = client.getHiveClient().getThriftClient()
              .add_partition_with_environment_context(partition, environmentContext);
      invalidateNonTransactionalTableIfExists(partition.getDbName(),
          partition.getTableName(),
          "add_partition_with_environment_context");
      return addedPartition;
    }
  }

  @Override
  @ServedFromCatalogd
  public int add_partitions(List<Partition> partitionList)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      int numPartitionsAdded =
          client.getHiveClient().getThriftClient().add_partitions(partitionList);
      if (numPartitionsAdded > 0) {
        Partition partition = partitionList.get(0);
        invalidateNonTransactionalTableIfExists(partition.getDbName(),
            partition.getTableName(), "add_partitions");
      }
      return numPartitionsAdded;
    }
  }

  @Override
  @ServedFromCatalogd
  public int add_partitions_pspec(List<PartitionSpec> list)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      int numPartitionsAdded =  client.getHiveClient()
              .getThriftClient().add_partitions_pspec(list);
      if (numPartitionsAdded > 0) {
        PartitionSpec partitionSpec = list.get(0);
        invalidateNonTransactionalTableIfExists(partitionSpec.getDbName(),
            partitionSpec.getTableName(), "add_partitions_pspec");
      }
      return numPartitionsAdded;
    }
  }

  @Override
  @ServedFromCatalogd
  public Partition append_partition(String dbname, String tblName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      Partition partition = client.getHiveClient().getThriftClient()
              .append_partition(dbname, tblName, partVals);
      invalidateNonTransactionalTableIfExists(dbname, tblName,
          "append_partition");
      return partition;
    }
  }

  @Override
  @ServedFromCatalogd
  public AddPartitionsResult add_partitions_req(AddPartitionsRequest addPartitionsRequest)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      AddPartitionsResult result =  client.getHiveClient().getThriftClient()
              .add_partitions_req(addPartitionsRequest);
      invalidateNonTransactionalTableIfExists(addPartitionsRequest.getDbName(),
          addPartitionsRequest.getTblName(), "add_partitions_req");
      return result;
    }
  }

  @Override
  @ServedFromCatalogd
  public Partition append_partition_with_environment_context(String dbname,
      String tblname,
      List<String> partVals, EnvironmentContext environmentContext)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      Partition partition =  client.getHiveClient().getThriftClient()
              .append_partition_with_environment_context(dbname, tblname,
                  partVals, environmentContext);
      invalidateNonTransactionalTableIfExists(dbname, tblname,
          "append_partition_with_environment_context");
      return partition;
    }
  }

  @Override
  @ServedFromCatalogd
  public Partition append_partition_by_name(String dbname, String tblname,
      String partName)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      Partition partition = client.getHiveClient().getThriftClient()
              .append_partition_by_name(dbname, tblname, partName);
      invalidateNonTransactionalTableIfExists(dbname, tblname,
          "append_partition_by_name");
      return partition;
    }
  }

  @Override
  @ServedFromCatalogd
  public Partition append_partition_by_name_with_environment_context(String dbname,
      String tblname, String partName, EnvironmentContext environmentContext)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      Partition partition =  client.getHiveClient().getThriftClient()
              .append_partition_by_name_with_environment_context(dbname, tblname,
                  partName, environmentContext);
      invalidateNonTransactionalTableIfExists(dbname, tblname,
          "append_partition_by_name_with_environment_context");
      return partition;
    }
  }

  @Override
  @ServedFromCatalogd
  public boolean drop_partition(String dbname, String tblname, List<String> partVals,
      boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      boolean partitionDropped = client.getHiveClient().getThriftClient()
              .drop_partition(dbname, tblname, partVals, deleteData);
      invalidateNonTransactionalTableIfExists(dbname, tblname,
          "drop_partition");
      return partitionDropped;
    }
  }

  @Override
  @ServedFromCatalogd
  public boolean drop_partition_with_environment_context(String dbname, String tblname,
      List<String> partNames, boolean deleteData, EnvironmentContext environmentContext)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      boolean partitionsDropped =  client.getHiveClient().getThriftClient()
              .drop_partition_with_environment_context(dbname, tblname,
                  partNames, deleteData, environmentContext);
      invalidateNonTransactionalTableIfExists(dbname, tblname,
          "drop_partition_with_environment_context");
      return partitionsDropped;
    }
  }

  @Override
  @ServedFromCatalogd
  public boolean drop_partition_by_name(String dbname, String tblname, String partName,
      boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      boolean partitionsDropped =
          client.getHiveClient().getThriftClient().drop_partition_by_name(dbname,
              tblname, partName, deleteData);
      invalidateNonTransactionalTableIfExists(dbname, tblname,
          "drop_partition_by_name");
      return partitionsDropped;
    }
  }

  @Override
  @ServedFromCatalogd
  public boolean drop_partition_by_name_with_environment_context(String dbName,
      String tableName,
      String partName, boolean deleteData, EnvironmentContext envContext)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      boolean partitionsDropped = client.getHiveClient().getThriftClient()
              .drop_partition_by_name_with_environment_context(dbName, tableName,
                  partName, deleteData, envContext);
      invalidateNonTransactionalTableIfExists(dbName, tableName,
          "drop_partition_by_name_with_environment_context");
      return partitionsDropped;
    }
  }

  @Override
  @ServedFromCatalogd
  public DropPartitionsResult drop_partitions_req(
      DropPartitionsRequest dropPartitionsRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      DropPartitionsResult result =
          client.getHiveClient().getThriftClient()
              .drop_partitions_req(dropPartitionsRequest);
      invalidateNonTransactionalTableIfExists(dropPartitionsRequest.getDbName(),
          dropPartitionsRequest.getTblName(), "drop_partitions_req");
      return result;
    }
  }

  @Override
  @ServedFromCatalogd
  public Partition exchange_partition(Map<String, String> partitionSpecMap,
      String sourcedb, String sourceTbl,
      String destDb, String destTbl)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      Partition partition = client.getHiveClient().getThriftClient()
          .exchange_partition(partitionSpecMap, sourcedb, sourceTbl, destDb,
              destTbl);
      String apiName = "exchange_partition";
      invalidateNonTransactionalTableIfExists(sourcedb, sourceTbl, apiName);
      invalidateNonTransactionalTableIfExists(destDb, destTbl, apiName);
      return partition;
    }
  }

  @Override
  @ServedFromCatalogd
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destDb,
      String destinationTableName)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      List<Partition> partitions =
          client.getHiveClient().getThriftClient()
              .exchange_partitions(partitionSpecs, sourceDb,
                  sourceTable, destDb, destinationTableName);
      String apiName = "exchange_partitions";
      invalidateNonTransactionalTableIfExists(sourceDb, sourceTable, apiName);
      invalidateNonTransactionalTableIfExists(destDb, destinationTableName, apiName);
      return partitions;
    }
  }

  /**
   * Util method to evaluate if the received exception needs to be thrown to the Client
   * based on the server configuration.
   *
   * @param cause   The underlying exception received from Catalog.
   * @param apiName The HMS API name which threw the given exception.
   * @throws TException Wrapped exception with the cause in case the given Exception is
   *                    not a TException. Else, throws the given TException.
   */
  protected void throwIfNoFallback(Exception cause, String apiName)
      throws TException {
    LOG.debug("Received exception while executing {}", apiName, cause);
    if (fallBackToHMSOnErrors_) return;
    if (cause instanceof TException) throw (TException) cause;
    // if this is not a TException we wrap it to a MetaException
    throw new MetaException(
        String.format(METAEXCEPTION_MSG_FORMAT, apiName, cause));
  }

  /**
   * This method gets the partitions for the given list of names from HMS. Additionally,
   * if the {@code getFileMetadata} flag is set in the request, it also computes the file
   * metadata and sets it in the partitions which are returned.
   *
   * @throws TException
   */
  public GetPartitionsByNamesResult get_partitions_by_names_req(
      GetPartitionsByNamesRequest getPartitionsByNamesRequest) throws TException {
    String tblName =
        getPartitionsByNamesRequest.getDb_name() + "." + getPartitionsByNamesRequest
            .getTbl_name();
    LOG.info(String
        .format(HMS_FALLBACK_MSG_FORMAT, HmsApiNameEnum.GET_PARTITION_BY_NAMES.apiName(),
            tblName));
    boolean getFileMetadata = getPartitionsByNamesRequest.isGetFileMetadata();
    GetPartitionsByNamesResult result;
    ValidWriteIdList validWriteIdList = null;
    ValidTxnList validTxnList = null;
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      result = client.getHiveClient().getThriftClient()
          .get_partitions_by_names_req(getPartitionsByNamesRequest);
      // if file-metadata is not request; return early
      if (!getFileMetadata) return result;
      // we don't really know if the requested partitions are for a transactional table
      // or not. Hence we should get the table from HMS to confirm.
      // TODO: may be we could assume that if ValidWriteIdList is not set, the table is
      // not transactional
      String[] parsedCatDbName = MetaStoreUtils
          .parseDbName(getPartitionsByNamesRequest.getDb_name(), serverConf_);
      Table tbl = client.getHiveClient().getTable(parsedCatDbName[0], parsedCatDbName[1],
          getPartitionsByNamesRequest.getTbl_name(),
          getPartitionsByNamesRequest.getValidWriteIdList());
      boolean isTransactional = tbl.getParameters() != null && AcidUtils
          .isTransactionalTable(tbl.getParameters());
      if (isTransactional) {
        if (getPartitionsByNamesRequest.getValidWriteIdList() == null) {
          throw new MetaException(
              "ValidWriteIdList is not set when requesting partitions for table " + tbl
                  .getDbName() + "." + tbl.getTableName());
        }
        validWriteIdList = MetastoreShim
            .getValidWriteIdListFromString(
                getPartitionsByNamesRequest.getValidWriteIdList());
        validTxnList = client.getHiveClient().getValidTxns();
      }
    }
    CatalogHmsAPIHelper
        .loadAndSetFileMetadataFromFs(validTxnList, validWriteIdList, result);
    return result;
  }

  @Override
  @ServedFromCatalogd
  public void alter_partition(String dbName, String tblName, Partition partition)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
              .alter_partition(dbName, tblName, partition);
      invalidateNonTransactionalTableIfExists(dbName, tblName,
          "alter_partition");
    }
  }

  @Override
  @ServedFromCatalogd
  public void alter_partitions(String dbName, String tblName, List<Partition> partitions)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
              .alter_partitions(dbName, tblName, partitions);
      invalidateNonTransactionalTableIfExists(dbName, tblName,
          "alter_partitions");
    }
  }

  @Override
  @ServedFromCatalogd
  public void alter_partitions_with_environment_context(String dbName, String tblName,
      List<Partition> list, EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
              .alter_partitions_with_environment_context(dbName, tblName,
                  list, environmentContext);
      invalidateNonTransactionalTableIfExists(dbName, tblName,
          "alter_partitions_with_environment_context");
    }
  }

  @Override
  @ServedFromCatalogd
  public AlterPartitionsResponse alter_partitions_req(
      AlterPartitionsRequest alterPartitionsRequest)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      AlterPartitionsResponse response =  client.getHiveClient().getThriftClient()
              .alter_partitions_req(alterPartitionsRequest);
      invalidateNonTransactionalTableIfExists(alterPartitionsRequest.getDbName(),
              alterPartitionsRequest.getTableName(), "alter_partitions_req");
      return response;
    }
  }

  @Override
  @ServedFromCatalogd
  public void alter_partition_with_environment_context(String dbName, String tblName,
      Partition partition, EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .alter_partition_with_environment_context(dbName, tblName, partition,
              environmentContext);
      invalidateNonTransactionalTableIfExists(dbName, tblName,
          "alter_partition_with_environment_context");
    }
  }

  @Override
  @ServedFromCatalogd
  public void rename_partition(String dbName, String tblName, List<String> list,
      Partition partition) throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .rename_partition(dbName, tblName, list, partition);
      invalidateNonTransactionalTableIfExists(dbName, tblName,
          "rename_partition");
    }
  }

  @Override
  @ServedFromCatalogd
  public RenamePartitionResponse rename_partition_req(
      RenamePartitionRequest renamePartitionRequest)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      RenamePartitionResponse response = client.getHiveClient().getThriftClient()
          .rename_partition_req(renamePartitionRequest);
      invalidateNonTransactionalTableIfExists(renamePartitionRequest.getDbName(),
          renamePartitionRequest.getTableName(), "rename_partition_req");
      return response;
    }
  }

  /**
   * For non transactional tables, invalidate the table from cache
   * if hms ddl apis are accessed from catalogd's metastore server.
   * Any subsequent get table request fetches the table from HMS and loads
   * it in cache. This ensures that any get_table/get_partition requests after ddl
   * operations on the same table return updated table. This behaviour
   * has a performance penalty (since table loading in cache takes time)
   * but ensures consistency. This change is behind catalogd server's
   * flag: invalidate_hms_cache_on_ddls which is enabled by default
   * It can be turned off if it becomes a performance bottleneck.
   * @param dbNameWithCatalog: Name of database which contains the table
   * @param tableName: Name of the table to invalidate
   * @param apiName: The reason to invalidate table from cache.
   */
  private void invalidateNonTransactionalTableIfExists(String dbNameWithCatalog,
      String tableName, String apiName) throws MetaException {
    // return immediately if flag invalidateCacheOnDDLs_ is false
    if (!invalidateCacheOnDDLs_) {
      LOG.debug("Not invalidating table {}.{} from catalogd cache because " +
              "invalidateCacheOnDDLs_ flag is set to {} ", dbNameWithCatalog,
          tableName, invalidateCacheOnDDLs_);
      return;
    }
    // Parse db name. Throw error if parsing fails.
    String dbName = dbNameWithCatalog;
    try {
      dbName = MetaStoreUtils.parseDbName(dbNameWithCatalog, serverConf_)[1];
    } catch (MetaException ex) {
      LOG.error("Successfully executed HMS api: {} but encountered error " +
              "when parsing dbName {} to invalidate/remove table from cache " +
              "with error message: {}", apiName, dbNameWithCatalog,
          ex.getMessage());
      throw ex;
    }
    org.apache.impala.catalog.Table catalogTbl= null;
    try {
      catalogTbl = catalog_.getTable(dbName, tableName);
    } catch (DatabaseNotFoundException ex) {
      LOG.debug(ex.getMessage());
      return;
    }
    if (catalogTbl == null) {
      LOG.debug("{}.{} does not exist", dbName, tableName);
      return;
    }
    if (catalogTbl instanceof IncompleteTable) {
      LOG.debug("table {} is already incomplete, not invalidating" +
              " it due to hms api: {}", catalogTbl.getFullName(),
          apiName);
      return;
    }
    Map<String, String> tblProperties = catalogTbl.getMetaStoreTable().getParameters();
    if (tblProperties == null || MetaStoreUtils.isTransactionalTable(tblProperties)) {
      LOG.debug("Table {} is transactional. " + "Not removing it " +
          "from catalogd cache", catalogTbl.getFullName());
      return;
    }

    LOG.debug("Invalidating non transactional table {} due to metastore api {}",
            catalogTbl.getFullName(), apiName);
    org.apache.impala.catalog.Table invalidatedCatalogTbl =
            catalog_.invalidateTableIfExists(dbName, tableName);
    if (invalidatedCatalogTbl != null) {
      LOG.info("Invalidated non transactional table {} from " +
              "catalogd cache due to metastore api: {}", catalogTbl.getFullName(),
          apiName);
    }
    return;
  }

  /**
   * This method is identical to invalidateNonTransactionalTableIfExists()
   * except that it removes(and not invalidates) table from the cache on
   * ddls like drop_table
   */
  private void removeNonTransactionalTableIfExists(String dbNameWithCatalog,
      String tableName, String apiName) throws MetaException {
    // return immediately if flag invalidateCacheOnDDLs_ is false
    if (!invalidateCacheOnDDLs_) {
      LOG.debug("Not removing table {}.{} from catalogd cache because " +
              "invalidateCacheOnDDLs_ flag is set to {} ", dbNameWithCatalog,
          tableName, invalidateCacheOnDDLs_);
      return;
    }
    // Parse db name. Throw error if parsing fails.
    String dbName = dbNameWithCatalog;
    try {
      dbName = MetaStoreUtils.parseDbName(dbNameWithCatalog, serverConf_)[1];
    } catch (MetaException ex) {
      LOG.error("Successfully executed HMS api: {} but encountered error " +
              "when parsing dbName {} to invalidate/remove table from cache " +
              "with error message: {}", apiName, dbNameWithCatalog,
          ex.getMessage());
      throw ex;
    }
    org.apache.impala.catalog.Table catalogTbl = null;
    try {
      catalogTbl = catalog_.getTable(dbName, tableName);
    } catch (DatabaseNotFoundException ex) {
      LOG.debug(ex.getMessage());
      return;
    }
    if (catalogTbl == null) {
      LOG.debug("{}.{} does not exist", dbName, tableName);
      return;
    }
    if (catalogTbl instanceof IncompleteTable) {
      LOG.debug("Removing incomplete table {} from cache " +
          "due to HMS API: ", catalogTbl.getFullName(), apiName);
      if (catalog_.removeTable(dbName, tableName) != null) {
        LOG.info("Removed incomplete table {} from cache due " +
            "to HMS API: ", catalogTbl.getFullName(), apiName);
      }
      return;
    }
    Map<String, String> tblProperties = catalogTbl.getMetaStoreTable().getParameters();
    if (tblProperties == null || MetaStoreUtils.isTransactionalTable(tblProperties)) {
      LOG.debug("Table {} is transactional. " +
          "Not removing it from catalogd cache", catalogTbl.getFullName());
      return;
    }
    LOG.debug("Removing non transactional table {} due to HMS api {}",
            catalogTbl.getFullName(), apiName);
    Reference<Boolean> tableFound = new Reference<>();
    Reference<Boolean> tableMatched = new Reference<>();
    // TODO: Move method removeTableIfExists to CatalogOpExecutor
    // as suggested in
    // IMPALA-10502 (patch: https://gerrit.cloudera.org/#/c/17308/)
    org.apache.impala.catalog.Table removedTable =
            catalog_.removeTableIfExists(catalogTbl.getMetaStoreTable(),
                tableFound, tableMatched);
    if (removedTable != null) {
      LOG.info("Removed non transactional table {} from catalogd cache due to " +
              "HMS api: {}", catalogTbl.getFullName(), apiName);
    }
    return;
  }

  /*
  This method is similar to invalidateNonTransactionalTableIfExists except that
  it is used only for alter_table apis. Atomically drops the old table and
  create a new table
   */
  private void renameNonTransactionalTableIfExists(String oldDbNameWithCatalog,
      String oldTableName, String newDbNameWithCatalog, String newTableName,
      String apiName) throws MetaException {
    // return immediately if flag invalidateCacheOnDDLs_ is false
    if (!invalidateCacheOnDDLs_) {
      LOG.debug("invalidateCacheOnDDLs_ flag is false, skipping cache " +
              "update for operation {} on table {}.{}", apiName,
          oldDbNameWithCatalog, oldTableName);
      return;
    }
    String toParse = null, oldDbName, newDbName;
    // Parse old and new db names. Throw error if parsing fails
    try {
      toParse = oldDbNameWithCatalog;
      oldDbName = MetaStoreUtils.parseDbName(toParse, serverConf_)[1];
      toParse = newDbNameWithCatalog;
      newDbName = MetaStoreUtils.parseDbName(toParse, serverConf_)[1];
    } catch (MetaException ex) {
      LOG.error("Successfully executed HMS api: {} but encountered error " +
              "when parsing dbName {}" + "with error message: {}",
          apiName, toParse, ex.getMessage());
      throw ex;
    }
    TTableName oldTable = new TTableName(oldDbName, oldTableName);
    TTableName newTable = new TTableName(newDbName, newTableName);
    String tableInfo = "old table " + oldDbName + "." + oldTableName +
        " to new table " + newDbName + "." + newTableName;
    LOG.debug("Renaming " + tableInfo);
    Pair<org.apache.impala.catalog.Table, org.apache.impala.catalog.Table> result =
        catalog_.renameTable(oldTable, newTable);
    if (result.first == null || result.second == null) {
      LOG.debug("Couldn't rename " + tableInfo);
    } else {
      LOG.info("Successfully renamed " + tableInfo);
    }
    return;
  }
}
