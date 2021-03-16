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

package org.apache.impala.service;

import static org.apache.impala.analysis.Analyzer.ACCESSTYPE_READWRITE;
import static org.apache.impala.catalog.operations.CatalogOperation.setStorageDescriptorFileFormat;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.impala.analysis.AlterTableSortByStmt;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationDelta;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogObject;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnNotFoundException;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.HiveStorageDescriptorFactory;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.PartitionNotFoundException;
import org.apache.impala.catalog.PartitionStatsUtil;
import org.apache.impala.catalog.RowFormat;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.Transaction;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.View;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.catalog.monitor.CatalogMonitor;
import org.apache.impala.catalog.monitor.CatalogOperationMetrics;
import org.apache.impala.catalog.operations.AlterDatabaseOperation;
import org.apache.impala.catalog.operations.AlterTableOperation;
import org.apache.impala.catalog.operations.AlterViewOperation;
import org.apache.impala.catalog.operations.CatalogOperation;
import org.apache.impala.catalog.operations.CreateDatabaseOperation;
import org.apache.impala.catalog.operations.CreateDatasourceOperation;
import org.apache.impala.catalog.operations.CreateFunctionOperation;
import org.apache.impala.catalog.operations.CreateTableLikeOperation;
import org.apache.impala.catalog.operations.CreateTableOperation;
import org.apache.impala.catalog.operations.CreateViewOperation;
import org.apache.impala.catalog.operations.DropDatabaseOperation;
import org.apache.impala.catalog.operations.DropStatsOperation;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.common.TransactionException;
import org.apache.impala.common.TransactionKeepalive.HeartbeatContext;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.JniCatalogConstants;
import org.apache.impala.thrift.TAlterDbParams;
import org.apache.impala.thrift.TAlterDbSetOwnerParams;
import org.apache.impala.thrift.TAlterTableAddColsParams;
import org.apache.impala.thrift.TAlterTableAddDropRangePartitionParams;
import org.apache.impala.thrift.TAlterTableAddPartitionParams;
import org.apache.impala.thrift.TAlterTableAlterColParams;
import org.apache.impala.thrift.TAlterTableDropColParams;
import org.apache.impala.thrift.TAlterTableDropPartitionParams;
import org.apache.impala.thrift.TAlterTableOrViewSetOwnerParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableReplaceColsParams;
import org.apache.impala.thrift.TAlterTableSetCachedParams;
import org.apache.impala.thrift.TAlterTableSetFileFormatParams;
import org.apache.impala.thrift.TAlterTableSetLocationParams;
import org.apache.impala.thrift.TAlterTableSetRowFormatParams;
import org.apache.impala.thrift.TAlterTableSetTblPropertiesParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TCatalogUpdateResult;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnName;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TCommentOnParams;
import org.apache.impala.thrift.TCopyTestCaseReq;
import org.apache.impala.thrift.TCreateDataSourceParams;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.thrift.TCreateDropRoleParams;
import org.apache.impala.thrift.TCreateFunctionParams;
import org.apache.impala.thrift.TCreateOrAlterViewParams;
import org.apache.impala.thrift.TCreateTableLikeParams;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDropDataSourceParams;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.impala.thrift.TDropFunctionParams;
import org.apache.impala.thrift.TDropStatsParams;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.impala.thrift.THdfsCachingOp;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TPartitionDef;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.thrift.TRangePartitionOperationType;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TResetMetadataResponse;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTableRowFormat;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TTestCaseData;
import org.apache.impala.thrift.TTruncateParams;
import org.apache.impala.thrift.TUpdateCatalogRequest;
import org.apache.impala.thrift.TUpdateCatalogResponse;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.AcidUtils.TblTransaction;
import org.apache.impala.util.CompressionUtil;
import org.apache.impala.util.DebugUtils;
import org.apache.impala.util.FunctionUtils;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.impala.util.MetaStoreUtil.TableInsertEventInfo;
import org.apache.thrift.TException;
import org.apache.zookeeper.Op.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to execute Catalog Operations, including DDL and refresh/invalidate
 * metadata requests. Acts as a bridge between the Thrift catalog operation requests
 * and the non-thrift Java Catalog objects.
 *
 * Updates are applied first to the Hive Metastore and only if they succeed, are then
 * applied to the catalog objects. To ensure consistency in the presence of failed HMS
 * updates, DDL operations should not directly modify the HMS objects of the catalog
 * objects but should operate on copies instead.
 *
 * The CatalogOpExecutor uses table-level or Db object level locking to protect table
 * metadata or database metadata respectively during concurrent modifications and is
 * responsible for assigning a new catalog version when a table/Db is modified
 * (e.g. alterTable() or alterDb()).
 *
 * The following locking protocol is employed to ensure that modifying
 * the table/Db metadata and assigning a new catalog version is performed atomically and
 * consistently in the presence of concurrent DDL operations. The following pattern
 * ensures that the catalog lock is never held for a long period of time, preventing
 * other DDL operations from making progress. This pattern only applies to single-table/Db
 * update operations and requires the use of fair table locks to prevent starvation.
 * Additionally, this locking protocol is also followed in case of CREATE/DROP
 * FUNCTION. In case of CREATE/DROP FUNCTION, we take the Db object lock since
 * certain FUNCTION are stored in the HMS database parameters. Using this approach
 * also makes sure that adding or removing functions from different databases do not
 * block each other.
 *
 *   DO {
 *     Acquire the catalog lock (see CatalogServiceCatalog.versionLock_)
 *     Try to acquire a table/Db lock
 *     IF the table/Db lock acquisition fails {
 *       Release the catalog lock
 *       YIELD()
 *     ELSE
 *       BREAK
 *   } WHILE (TIMEOUT);
 *
 *   If (TIMEOUT) report error
 *
 *   Increment and get a new catalog version
 *   Release the catalog lock
 *   Modify table/Db metadata
 *   Release table/Db lock
 *
 * Note: The getCatalogObjects() function is the only case where this locking pattern is
 * not used since it accesses multiple catalog entities in order to compute a snapshot
 * of catalog metadata.
 *
 * Operations that CREATE/DROP catalog objects such as tables and databases
 * (except for functions, see above) employ the following locking protocol:
 * 1. Acquire the metastoreDdlLock_
 * 2. Update the Hive Metastore
 * 3. Increment and get a new catalog version
 * 4. Update the catalog
 * 5. Grant/revoke owner privilege if authorization with ownership is enabled.
 * 6. Release the metastoreDdlLock_
 *
 *
 * It is imperative that other operations that need to hold both the catalog lock and
 * table locks at the same time follow the same locking protocol and acquire these
 * locks in that particular order. Also, operations that modify table metadata
 * (e.g. alter table statements) should not acquire the metastoreDdlLock_.
 *
 * TODO: Refactor the CatalogOpExecutor and CatalogServiceCatalog classes and consolidate
 * the locking protocol into a single class.
 *
 * TODO: Improve catalog's consistency guarantees by using a hierarchical locking scheme.
 * Currently, only concurrent modidications to table metadata are guaranteed to be
 * serialized. Concurrent DDL operations that DROP/ADD catalog objects,
 * especially in the presence of INVALIDATE METADATA and REFRESH, are not guaranteed to
 * be consistent (see IMPALA-2774).
 *
 * TODO: Create a Hive Metastore utility class to move code that interacts with the
 * metastore out of this class.
 */
public class CatalogOpExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogOpExecutor.class);

  private final CatalogServiceCatalog catalog_;
  private final AuthorizationConfig authzConfig_;
  private final AuthorizationManager authzManager_;

  // A singleton monitoring class that keeps track of the catalog usage metrics.
  private final CatalogOperationMetrics catalogOpMetric_ =
      CatalogMonitor.INSTANCE.getCatalogOperationMetrics();

  public CatalogOpExecutor(CatalogServiceCatalog catalog, AuthorizationConfig authzConfig,
      AuthorizationManager authzManager) throws ImpalaException {
    Preconditions.checkNotNull(authzManager);
    catalog_ = Preconditions.checkNotNull(catalog);
    authzConfig_ = Preconditions.checkNotNull(authzConfig);
    authzManager_ = Preconditions.checkNotNull(authzManager);
  }

  public AuthorizationConfig getAuthzConfig() { return authzConfig_; }

  public CatalogServiceCatalog getCatalog() { return catalog_; }

  public AuthorizationManager getAuthzManager() { return authzManager_; }

  public TDdlExecResponse execDdlRequest(TDdlExecRequest ddlRequest)
      throws ImpalaException {
    TDdlExecResponse response = new TDdlExecResponse();
    response.setResult(new TCatalogUpdateResult());
    response.getResult().setCatalog_service_id(JniCatalog.getServiceId());
    User requestingUser = null;
    boolean wantMinimalResult = false;
    if (ddlRequest.isSetHeader()) {
      TCatalogServiceRequestHeader header = ddlRequest.getHeader();
      if (header.isSetRequesting_user()) {
        requestingUser = new User(ddlRequest.getHeader().getRequesting_user());
      }
      wantMinimalResult = ddlRequest.getHeader().isWant_minimal_response();
    }
    Optional<TTableName> tTableName = Optional.empty();
    TDdlType ddl_type = ddlRequest.ddl_type;
    try {
      boolean syncDdl = ddlRequest.isSync_ddl();
      switch (ddl_type) {
        case ALTER_DATABASE:
          TAlterDbParams alter_db_params = ddlRequest.getAlter_db_params();
          tTableName = Optional.of(new TTableName(alter_db_params.db, ""));
          catalogOpMetric_.increment(ddl_type, tTableName);
          new AlterDatabaseOperation(ddlRequest, response, this, wantMinimalResult)
              .execute();
          break;
        case ALTER_TABLE:
          TAlterTableParams alter_table_params = ddlRequest.getAlter_table_params();
          tTableName = Optional.of(alter_table_params.getTable_name());
          catalogOpMetric_.increment(ddl_type, tTableName);
          new AlterTableOperation(ddlRequest, response, this, wantMinimalResult)
              .execute();
          break;
        case ALTER_VIEW:
          TCreateOrAlterViewParams alter_view_params = ddlRequest.getAlter_view_params();
          tTableName = Optional.of(alter_view_params.getView_name());
          catalogOpMetric_.increment(ddl_type, tTableName);
          new AlterViewOperation(ddlRequest, response, this, wantMinimalResult).execute();
          break;
        case CREATE_DATABASE:
          TCreateDbParams create_db_params = ddlRequest.getCreate_db_params();
          tTableName = Optional.of(new TTableName(create_db_params.db, ""));
          catalogOpMetric_.increment(ddl_type, tTableName);
          new CreateDatabaseOperation(ddlRequest, response, this, wantMinimalResult)
              .execute();
          break;
        case CREATE_TABLE_AS_SELECT:
        case CREATE_TABLE:
          TCreateTableParams create_table_as_select_params =
              ddlRequest.getCreate_table_params();
          tTableName = Optional.of(create_table_as_select_params.getTable_name());
          catalogOpMetric_.increment(ddl_type, tTableName);
          new CreateTableOperation(ddlRequest, response, this, wantMinimalResult)
              .execute();
          break;
        case CREATE_TABLE_LIKE:
          TCreateTableLikeParams create_table_like_params =
              ddlRequest.getCreate_table_like_params();
          tTableName = Optional.of(create_table_like_params.getTable_name());
          catalogOpMetric_.increment(ddl_type, tTableName);
          new CreateTableLikeOperation(ddlRequest, response, this, wantMinimalResult)
              .execute();
          break;
        case CREATE_VIEW:
          TCreateOrAlterViewParams create_view_params =
              ddlRequest.getCreate_view_params();
          tTableName = Optional.of(create_view_params.getView_name());
          catalogOpMetric_.increment(ddl_type, tTableName);
          new CreateViewOperation(ddlRequest, response, this, wantMinimalResult)
              .execute();
          break;
        case CREATE_FUNCTION:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          new CreateFunctionOperation(ddlRequest, response, this, wantMinimalResult)
              .execute();
          break;
        case CREATE_DATA_SOURCE:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          new CreateDatasourceOperation(ddlRequest, response, this, wantMinimalResult)
              .execute();
          break;
        case COMPUTE_STATS:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          Preconditions.checkState(false, "Compute stats should trigger an ALTER TABLE.");
          break;
        case DROP_STATS:
          TDropStatsParams drop_stats_params = ddlRequest.getDrop_stats_params();
          tTableName = Optional.of(drop_stats_params.getTable_name());
          catalogOpMetric_.increment(ddl_type, tTableName);
          new DropStatsOperation(ddlRequest, response, this, wantMinimalResult).execute();
          break;
        case DROP_DATABASE:
          TDropDbParams drop_db_params = ddlRequest.getDrop_db_params();
          tTableName = Optional.of(new TTableName(drop_db_params.getDb(), ""));
          catalogOpMetric_.increment(ddl_type, tTableName);
          new DropDatabaseOperation(ddlRequest, response, this, wantMinimalResult)
              .execute();
          break;
        case DROP_TABLE:
        case DROP_VIEW:
          TDropTableOrViewParams drop_table_or_view_params =
              ddlRequest.getDrop_table_or_view_params();
          tTableName = Optional.of(drop_table_or_view_params.getTable_name());
          catalogOpMetric_.increment(ddl_type, tTableName);
          // Dropped tables and views are already returned as minimal results, so don't
          // need to pass down wantMinimalResult here.
          dropTableOrView(drop_table_or_view_params, response);
          break;
        case TRUNCATE_TABLE:
          TTruncateParams truncate_params = ddlRequest.getTruncate_params();
          tTableName = Optional.of(truncate_params.getTable_name());
          catalogOpMetric_.increment(ddl_type, tTableName);
          truncateTable(truncate_params, wantMinimalResult, response);
          break;
        case DROP_FUNCTION:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          dropFunction(ddlRequest.getDrop_fn_params(), response);
          break;
        case DROP_DATA_SOURCE:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          dropDataSource(ddlRequest.getDrop_data_source_params(), response);
          break;
        case CREATE_ROLE:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          createRole(requestingUser, ddlRequest.getCreate_drop_role_params(), response);
          break;
        case DROP_ROLE:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          dropRole(requestingUser, ddlRequest.getCreate_drop_role_params(), response);
          break;
        case GRANT_ROLE:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          grantRoleToGroup(
              requestingUser, ddlRequest.getGrant_revoke_role_params(), response);
          break;
        case REVOKE_ROLE:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          revokeRoleFromGroup(
              requestingUser, ddlRequest.getGrant_revoke_role_params(), response);
          break;
        case GRANT_PRIVILEGE:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          grantPrivilege(
              ddlRequest.getHeader(), ddlRequest.getGrant_revoke_priv_params(), response);
          break;
        case REVOKE_PRIVILEGE:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          revokePrivilege(
              ddlRequest.getHeader(), ddlRequest.getGrant_revoke_priv_params(), response);
          break;
        case COMMENT_ON:
          TCommentOnParams comment_on_params = ddlRequest.getComment_on_params();
          tTableName = Optional.of(new TTableName("", ""));
          alterCommentOn(comment_on_params, response, tTableName, wantMinimalResult);
          break;
        case COPY_TESTCASE:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          copyTestCaseData(ddlRequest.getCopy_test_case_params(), response);
          break;
        default:
          catalogOpMetric_.increment(ddl_type, Optional.empty());
          throw new IllegalStateException(
              "Unexpected DDL exec request type: " + ddl_type);
      }

      // If SYNC_DDL is set, set the catalog update that contains the results of this DDL
      // operation. The version of this catalog update is returned to the requesting
      // impalad which will wait until this catalog update has been broadcast to all the
      // coordinators.
      if (syncDdl) {
        response.getResult().setVersion(
            catalog_.waitForSyncDdlVersion(response.getResult()));
      }

      // At this point, the operation is considered successful. If any errors occurred
      // during execution, this function will throw an exception and the CatalogServer
      // will handle setting a bad status code.
      response.getResult().setStatus(new TStatus(TErrorCode.OK, new ArrayList<String>()));
    } finally {
      catalogOpMetric_.decrement(ddl_type, tTableName);
    }
    return response;
  }

  /**
   * Loads the testcase metadata from the request into the catalog cache and returns
   * the query statement this input testcase corresponds to. When loading the table and
   * database objects, this method overwrites any existing tables or databases with the
   * same name. However, these overwrites are *not* persistent. The old table/db
   * states can be recovered by blowing away the cache using INVALIDATE METADATA.
   */
  @VisibleForTesting
  public String copyTestCaseData(
      TCopyTestCaseReq request, TDdlExecResponse response)
      throws ImpalaException {
    Path inputPath = new Path(Preconditions.checkNotNull(request.input_path));
    // Read the data from the source FS.
    FileSystem fs;
    FSDataInputStream in;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      fs = FileSystemUtil.getFileSystemForPath(inputPath);
      in = fs.open(inputPath);
      IOUtils.copyBytes(in, out, fs.getConf(), /*close streams*/true);
    } catch (IOException e) {
      throw new ImpalaRuntimeException(String.format("Error reading test case data from" +
          " path: %s", inputPath), e);
    }
    byte[] decompressedBytes = CompressionUtil.deflateDecompress(out.toByteArray());
    TTestCaseData testCaseData = new TTestCaseData();
    try {
      JniUtil.deserializeThrift(testCaseData, decompressedBytes);
    } catch (ImpalaException e) {
      throw new CatalogException(String.format("Error deserializing the testcase data " +
          "at path %s. File data may be corrupt or incompatible with the current version "
          + "of Impala.", inputPath.toString()),e);
    }

    // Add the databases first, followed by the table and views information.
    // Overwrites any existing Db/Table objects with name clashes. Since we overwrite
    // the state in-memory and do not flush it to HMS, the older state can be recovered
    // by loading everything back from HMS. For ex: INVALIDATE METADATA.
    int numDbsAdded = 0;
    if (testCaseData.getDbs() != null) {
      for (TDatabase thriftDb : testCaseData.getDbs()) {
        Db db = Db.fromTDatabase(thriftDb);
        // Set a new version to force an overwrite if a Db already exists with the same
        // name.
        db.setCatalogVersion(catalog_.incrementAndGetCatalogVersion());
        Db ret = catalog_.addDb(db.getName(), db.getMetaStoreDb());
        if (ret != null) {
          ++numDbsAdded;
          response.result.addToUpdated_catalog_objects(db.toTCatalogObject());
        }
      }
    }

    int numTblsAdded = 0;
    int numViewsAdded = 0;
    if (testCaseData.getTables_and_views() != null) {
      for (TTable tTable : testCaseData.tables_and_views) {
        Db db = catalog_.getDb(tTable.db_name);
        // Db should have been created by now.
        Preconditions.checkNotNull(db, String.format("Missing db %s", tTable.db_name));
        Table t = Table.fromThrift(db, tTable);
        // Set a new version to force an overwrite if a table already exists with the same
        // name.
        t.setCatalogVersion(catalog_.incrementAndGetCatalogVersion());
        catalog_.addTable(db, t);
        if (t instanceof View) {
          ++numViewsAdded;
        } else {
          ++numTblsAdded;
        }
        // The table lock is needed here since toTCatalogObject() calls Table#toThrift()
        // which expects the current thread to hold this lock. For more details refer
        // to IMPALA-4092.
        t.takeReadLock();
        try {
          response.result.addToUpdated_catalog_objects(t.toTCatalogObject());
        } finally {
          t.releaseReadLock();
        }
      }
    }
    StringBuilder responseStr = new StringBuilder();
    responseStr.append(String.format("Testcase generated using Impala version %s. ",
        testCaseData.getImpala_version()));
    responseStr.append(String.format(
        "%d db(s), %d table(s) and %d view(s) imported for query: ", numDbsAdded,
        numTblsAdded, numViewsAdded));
    responseStr.append("\n\n").append(testCaseData.getQuery_stmt());
    LOG.info(String.format("%s. Testcase path: %s", responseStr, inputPath));
    addSummary(response, responseStr.toString());
    return testCaseData.getQuery_stmt();
  }

  /**
   * Loads the metadata of a table 'tbl' and assigns a new catalog version.
   * 'reloadFileMetadata', 'reloadTableSchema', and 'partitionsToUpdate'
   * are used only for HdfsTables and control which metadata to reload.
   * Throws a CatalogException if there is an error loading table metadata.
   */
  public void loadTableMetadata(Table tbl, long newCatalogVersion,
      boolean reloadFileMetadata, boolean reloadTableSchema,
      Set<String> partitionsToUpdate, String reason) throws CatalogException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          getMetaStoreTable(msClient, tbl);
      if (tbl instanceof HdfsTable) {
        ((HdfsTable) tbl).load(true, msClient.getHiveClient(), msTbl,
            reloadFileMetadata, reloadTableSchema, false, partitionsToUpdate, null,
            reason);
      } else {
        tbl.load(true, msClient.getHiveClient(), msTbl, reason);
      }
    }
    tbl.setCatalogVersion(newCatalogVersion);
  }

  private void dropDataSource(TDropDataSourceParams params, TDdlExecResponse resp)
      throws ImpalaException {
    // TODO(IMPALA-7131): support data sources with LocalCatalog.
    if (LOG.isTraceEnabled()) LOG.trace("Drop DATA SOURCE: " + params.toString());
    DataSource dataSource = catalog_.removeDataSource(params.getData_source());
    if (dataSource == null) {
      if (!params.if_exists) {
        throw new ImpalaRuntimeException("Data source " + params.getData_source() +
            " does not exists.");
      }
      addSummary(resp, "Data source does not exist.");
      // No data source was removed.
      resp.result.setVersion(catalog_.getCatalogVersion());
      return;
    }
    resp.result.addToRemoved_catalog_objects(dataSource.toTCatalogObject());
    resp.result.setVersion(dataSource.getCatalogVersion());
    addSummary(resp, "Data source has been dropped.");
  }


  /**
   * Drops a table or view from the metastore and removes it from the catalog.
   * Also drops all associated caching requests on the table and/or table's partitions,
   * uncaching all table data. If params.purge is true, table data is permanently
   * deleted.
   * In case of transactional tables acquires an exclusive HMS table lock before
   * executing the drop operation.
   */
  private void dropTableOrView(TDropTableOrViewParams params, TDdlExecResponse resp)
      throws ImpalaException {


    try {
      dropTableOrViewInternal(params, tableName, resp);
    } finally {
      if (lockId > 0) catalog_.releaseTableLock(lockId);
    }
  }

  /**
   * Helper function for dropTableOrView().
   */
  private void dropTableOrViewInternal(TDropTableOrViewParams params,
      TableName tableName, TDdlExecResponse resp) throws ImpalaException {
    TCatalogObject removedObject = new TCatalogObject();
    synchronized (metastoreDdlLock_) {
      Db db = catalog_.getDb(params.getTable_name().db_name);
      if (db == null) {
        String dbNotExist = "Database does not exist: " + params.getTable_name().db_name;
        if (params.if_exists) {
          addSummary(resp, dbNotExist);
          return;
        }
        throw new CatalogException(dbNotExist);
      }
      Table existingTbl = db.getTable(params.getTable_name().table_name);
      if (existingTbl == null) {
        if (params.if_exists) {
          addSummary(resp, (params.is_table ? "Table " : "View ") + "does not exist.");
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
          addSummary(resp, "Drop " + (params.is_table ? "table " : "view ") +
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
        IcebergCatalogOpExecutor.dropTable((IcebergTable)existingTbl, params.if_exists);
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
      addSummary(resp, (params.is_table ? "Table " : "View ") + "has been dropped.");

      Table table = catalog_.removeTable(params.getTable_name().db_name,
          params.getTable_name().table_name);
      if (table == null) {
        // Nothing was removed from the catalogd's cache.
        resp.result.setVersion(catalog_.getCatalogVersion());
        return;
      }
      resp.result.setVersion(table.getCatalogVersion());
      uncacheTable(table);
      if (table.getMetaStoreTable() != null) {
        if (authzConfig_.isEnabled()) {
          authzManager_.updateTableOwnerPrivilege(params.server_name,
              table.getDb().getName(), table.getName(),
              table.getMetaStoreTable().getOwner(),
              table.getMetaStoreTable().getOwnerType(), /* newOwner */ null,
              /* newOwnerType */ null, resp);
        }
      }
    }
    removedObject.setType(TCatalogObjectType.TABLE);
    removedObject.setTable(new TTable());
    removedObject.getTable().setTbl_name(tableName.getTbl());
    removedObject.getTable().setDb_name(tableName.getDb());
    removedObject.setCatalog_version(resp.result.getVersion());
    resp.result.addToRemoved_catalog_objects(removedObject);
  }


  /**
   * Truncate a table by deleting all files in its partition directories, and dropping
   * all column and table statistics. Acquires a table lock to protect against
   * concurrent table modifications.
   * TODO truncate specified partitions.
   */
  private void truncateTable(TTruncateParams params, boolean wantMinimalResult,
      TDdlExecResponse resp) throws ImpalaException {
    TTableName tblName = params.getTable_name();
    Table table = null;
    try {
      table = getExistingTable(tblName.getDb_name(), tblName.getTable_name(),
          "Load for TRUNCATE TABLE");
    } catch (TableNotFoundException e) {
      if (params.if_exists) {
        addSummary(resp, "Table does not exist.");
        return;
      }
      throw e;
    }
    Preconditions.checkNotNull(table);
    if (!(table instanceof FeFsTable)) {
      throw new CatalogException(
          String.format("TRUNCATE TABLE not supported on non-HDFS table: %s",
          table.getFullName()));
    }
    // Lock table to check transactional properties.
    // If non-transactional, the lock will be held during truncation.
    // If transactional, the lock will be released for some time to acquire the HMS Acid
    // lock. It's safe because transactional -> non-transactional conversion is not
    // allowed.
    tryWriteLock(table, "truncating");
    try {
      long newCatalogVersion = 0;
      try {
        if (AcidUtils.isTransactionalTable(table.getMetaStoreTable().getParameters())) {
          newCatalogVersion = truncateTransactionalTable(params, table);
        } else if (table instanceof FeIcebergTable) {
          newCatalogVersion = truncateIcebergTable(params, table);
        } else {
          newCatalogVersion = truncateNonTransactionalTable(params, table);
        }
      } catch (Exception e) {
        String fqName = tblName.db_name + "." + tblName.table_name;
        throw new CatalogException(String.format("Failed to truncate table: %s.\n" +
            "Table may be in a partially truncated state.", fqName), e);
      }
      Preconditions.checkState(newCatalogVersion > 0);
      addSummary(resp, "Table has been truncated.");
      loadTableMetadata(table, newCatalogVersion, true, true, null, "TRUNCATE");
      addTableToCatalogUpdate(table, wantMinimalResult, resp.result);
    } finally {
      UnlockWriteLockIfErronouslyLocked();
      if (table.isWriteLockedByCurrentThread()) {
        table.releaseWriteLock();
      }
    }
  }

  /**
   * Truncates a transactional table. It creates new empty base directories in all
   * partitions of the table. That way queries started earlier can still read a
   * valid snapshot version of the data. HMS's cleaner should remove obsolete
   * directories later.
   * After that empty directory creation it removes stats-related parameters of
   * the table and its partitions.
   */
  private long truncateTransactionalTable(TTruncateParams params, Table table)
      throws ImpalaException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    Preconditions.checkState(catalog_.getLock().isWriteLockedByCurrentThread());
    catalog_.getLock().writeLock().unlock();
    TableName tblName = TableName.fromThrift(params.getTable_name());
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      long newCatalogVersion = 0;
      IMetaStoreClient hmsClient = msClient.getHiveClient();
      HeartbeatContext ctx = new HeartbeatContext(
          String.format("Truncate table %s.%s", tblName.getDb(), tblName.getTbl()),
          System.nanoTime());
      try (Transaction txn = catalog_.openTransaction(hmsClient, ctx)) {
        Preconditions.checkState(txn.getId() > 0);
        // We need to release catalog table lock here, because HMS Acid table lock
        // must be locked in advance to avoid dead lock.
        table.releaseWriteLock();
        //TODO: if possible, set DataOperationType to something better than NO_TXN.
        catalog_.lockTableInTransaction(tblName.getDb(), tblName.getTbl(), txn,
            DataOperationType.NO_TXN, ctx);
        tryWriteLock(table, "truncating");
        newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
        catalog_.getLock().writeLock().unlock();
        TblTransaction tblTxn = MetastoreShim.createTblTransaction(hmsClient,
            table.getMetaStoreTable(), txn.getId());
        HdfsTable hdfsTable = (HdfsTable) table;
        // if the table is replicated we should use the HMS API to truncate it so that
        // if moves the files into in replication change manager location which is later
        // used for replication.
        if (isTableBeingReplicated(hmsClient, hdfsTable)) {
          Stopwatch sw = Stopwatch.createStarted();
          String dbName = Preconditions.checkNotNull(hdfsTable.getDb()).getName();
          hmsClient.truncateTable(dbName, hdfsTable.getName(), null, tblTxn.validWriteIds,
              tblTxn.writeId);
          LOG.debug("Time taken to truncate table {} using HMS API: {} msec",
              hdfsTable.getFullName(), sw.stop().elapsed(TimeUnit.MILLISECONDS));
        } else {
          Collection<? extends FeFsPartition> parts =
              FeCatalogUtils.loadAllPartitions(hdfsTable);
          createEmptyBaseDirectories(parts, tblTxn.writeId);
          // Currently Impala cannot update the statistics properly. So instead of
          // writing correct stats, let's just remove COLUMN_STATS_ACCURATE parameter from
          // each partition.
          // TODO(IMPALA-8883): properly update statistics
          List<org.apache.hadoop.hive.metastore.api.Partition> hmsPartitions =
              Lists.newArrayListWithCapacity(parts.size());
          if (table.getNumClusteringCols() > 0) {
            for (FeFsPartition part : parts) {
              org.apache.hadoop.hive.metastore.api.Partition hmsPart =
                  ((HdfsPartition) part).toHmsPartition();
              Preconditions.checkNotNull(hmsPart);
              if (hmsPart.getParameters() != null) {
                hmsPart.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
                hmsPartitions.add(hmsPart);
              }
            }
          }
          // For partitioned tables we need to alter all the partitions in HMS.
          if (!hmsPartitions.isEmpty()) {
            unsetPartitionsColStats(table.getMetaStoreTable(), hmsPartitions, tblTxn);
          }
          // Remove COLUMN_STATS_ACCURATE property from the table.
          unsetTableColStats(table.getMetaStoreTable(), tblTxn);
        }
        txn.commit();
      }
      return newCatalogVersion;
    } catch (Exception e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "truncateTable"), e);
    }
  }

  /**
   * Helper method to check if the database which table belongs to is a source of
   * Hive replication. We cannot rely on the Db object here due to eventual nature of
   * cache updates.
   */
  private boolean isTableBeingReplicated(IMetaStoreClient metastoreClient,
      HdfsTable tbl) throws CatalogException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    String dbName = tbl.getDb().getName();
    try {
      Database db = metastoreClient.getDatabase(dbName);
      if (!db.isSetParameters()) return false;
      return org.apache.commons.lang.StringUtils
          .isNotEmpty(db.getParameters().get("repl.source.for"));
    } catch (TException tException) {
      throw new CatalogException(
          String.format("Could not determine if the table %s is a replication source",
          tbl.getFullName()), tException);
    }
  }

  private long truncateIcebergTable(TTruncateParams params, Table table)
      throws Exception {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    Preconditions.checkState(catalog_.getLock().isWriteLockedByCurrentThread());
    Preconditions.checkState(table instanceof FeIcebergTable);
    long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
    FeIcebergTable iceTable = (FeIcebergTable)table;
    dropColumnStats(table);
    dropTableStats(table);
    IcebergCatalogOpExecutor.truncateTable(iceTable);
    return newCatalogVersion;
  }

  private long truncateNonTransactionalTable(TTruncateParams params, Table table)
      throws Exception {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    Preconditions.checkState(catalog_.getLock().isWriteLockedByCurrentThread());
    long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
    HdfsTable hdfsTable = (HdfsTable) table;
    // if the table is being replicated we issue the HMS API to truncate the table
    // since it generates additional events which are used by Hive Replication.
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      if (isTableBeingReplicated(metaStoreClient.getHiveClient(), hdfsTable)) {
        String dbName = Preconditions.checkNotNull(hdfsTable.getDb()).getName();
        Stopwatch sw = Stopwatch.createStarted();
        metaStoreClient.getHiveClient().truncateTable(dbName, hdfsTable.getName(), null);
        LOG.debug("Time taken to truncate table {} using HMS API: {} msec",
            hdfsTable.getFullName(), sw.stop().elapsed(TimeUnit.MILLISECONDS));
        return newCatalogVersion;
      }
    }
    Collection<? extends FeFsPartition> parts = FeCatalogUtils
        .loadAllPartitions(hdfsTable);
    for (FeFsPartition part : parts) {
      FileSystemUtil.deleteAllVisibleFiles(new Path(part.getLocation()));
    }
    dropColumnStats(table);
    dropTableStats(table);
    return newCatalogVersion;
  }

  /**
   * Creates new empty base directories for an ACID table. The directories won't be
   * really empty, they will contain the "empty" file. It's needed because
   * FileSystemUtil.listFiles() doesn't see empty directories. See IMPALA-8739.
   * @param partitions the partitions in which we create new directories.
   * @param writeId the write id of the new base directory.
   * @throws IOException
   */
  private void createEmptyBaseDirectories(
      Collection<? extends FeFsPartition> partitions, long writeId) throws IOException {
    for (FeFsPartition part: partitions) {
      Path partPath = new Path(part.getLocation());
      FileSystem fs = FileSystemUtil.getFileSystemForPath(partPath);
      String baseDirStr =
          part.getLocation() + Path.SEPARATOR + "base_" + String.valueOf(writeId);
      fs.mkdirs(new Path(baseDirStr));
      String emptyFile = baseDirStr + Path.SEPARATOR + "empty";
      fs.create(new Path(emptyFile)).close();
    }
  }

  private void dropFunction(TDropFunctionParams params, TDdlExecResponse resp)
      throws ImpalaException {
    FunctionName fName = FunctionName.fromThrift(params.fn_name);
    Db db = catalog_.getDb(fName.getDb());
    if (db == null) {
      if (!params.if_exists) {
        throw new CatalogException("Database: " + fName.getDb()
            + " does not exist.");
      }
      addSummary(resp, "Database does not exist.");
      return;
    }

    tryLock(db, "dropping function " + fName);
    // Get a new catalog version to assign to the database being altered. This is
    // needed for events processor as this method creates alter database events.
    long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
    try {
      List<TCatalogObject> removedFunctions = Lists.newArrayList();
      if (!params.isSetSignature()) {
        dropJavaFunctionFromHms(fName.getDb(), fName.getFunction(), params.if_exists);
        for (Function fn : db.getFunctions(fName.getFunction())) {
          if (fn.getBinaryType() != TFunctionBinaryType.JAVA
              || !fn.isPersistent()) {
            continue;
          }
          Preconditions.checkNotNull(catalog_.removeFunction(fn));
          removedFunctions.add(fn.toTCatalogObject());
        }
      } else {
        ArrayList<Type> argTypes = Lists.newArrayList();
        for (TColumnType t : params.arg_types) {
          argTypes.add(Type.fromThrift(t));
        }
        Function desc = new Function(fName, argTypes, Type.INVALID, false);
        Function fn = catalog_.removeFunction(desc);
        if (fn == null) {
          if (!params.if_exists) {
            throw new CatalogException(
                "Function: " + desc.signatureString() + " does not exist.");
          }
        } else {
          addCatalogServiceIdentifiers(db.getMetaStoreDb(),
              catalog_.getCatalogServiceId(), newCatalogVersion);
          // Flush DB changes to metastore
          applyAlterDatabase(db.getMetaStoreDb());
          removedFunctions.add(fn.toTCatalogObject());
          // now that HMS alter operation has succeeded, add this version to list of
          // inflight events in catalog database if event processing is enabled.
          catalog_.addVersionsForInflightEvents(db, newCatalogVersion);
        }
      }

      if (!removedFunctions.isEmpty()) {
        addSummary(resp, "Function has been dropped.");
        resp.result.setRemoved_catalog_objects(removedFunctions);
      } else {
        addSummary(resp, "Function does not exist.");
      }
      resp.result.setVersion(catalog_.getCatalogVersion());
    } finally {
      db.getLock().unlock();
    }
  }

  /**
   * Drops the given function from Hive metastore. Returns true if successful
   * and false if the function does not exist and ifExists is true.
   */
  public boolean dropJavaFunctionFromHms(String db, String fn, boolean ifExists)
      throws ImpalaRuntimeException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().dropFunction(db, fn);
    } catch (NoSuchObjectException e) {
      if (!ifExists) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "dropFunction"), e);
      }
      return false;
    } catch (TException e) {
      LOG.error("Error executing dropFunction() metastore call: " + fn, e);
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "dropFunction"), e);
    }
    return true;
  }

  /**
   * Creates a role on behalf of the requestingUser.
   */
  private void createRole(User requestingUser,
      TCreateDropRoleParams createDropRoleParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    Preconditions.checkNotNull(createDropRoleParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(!createDropRoleParams.isIs_drop());
    authzManager_.createRole(requestingUser, createDropRoleParams, resp);
    addSummary(resp, "Role has been created.");
  }

  /**
   * Drops a role on behalf of the requestingUser.
   */
  private void dropRole(User requestingUser,
      TCreateDropRoleParams createDropRoleParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    Preconditions.checkNotNull(createDropRoleParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(createDropRoleParams.isIs_drop());
    authzManager_.dropRole(requestingUser, createDropRoleParams, resp);
    addSummary(resp, "Role has been dropped.");
  }

  /**
   * Grants a role to the given group on behalf of the requestingUser.
   */
  private void grantRoleToGroup(User requestingUser,
      TGrantRevokeRoleParams grantRevokeRoleParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    Preconditions.checkNotNull(grantRevokeRoleParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(grantRevokeRoleParams.isIs_grant());
    authzManager_.grantRoleToGroup(requestingUser, grantRevokeRoleParams, resp);
    addSummary(resp, "Role has been granted.");
  }

  /**
   * Revokes a role from the given group on behalf of the requestingUser.
   */
  private void revokeRoleFromGroup(User requestingUser,
      TGrantRevokeRoleParams grantRevokeRoleParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(requestingUser);
    Preconditions.checkNotNull(grantRevokeRoleParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(!grantRevokeRoleParams.isIs_grant());
    authzManager_.revokeRoleFromGroup(requestingUser, grantRevokeRoleParams, resp);
    addSummary(resp, "Role has been revoked.");
  }

  /**
   * Grants one or more privileges to role on behalf of the requestingUser.
   */
  private void grantPrivilege(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams grantRevokePrivParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(header);
    Preconditions.checkNotNull(grantRevokePrivParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(grantRevokePrivParams.isIs_grant());

    switch (grantRevokePrivParams.principal_type) {
      case ROLE:
        authzManager_.grantPrivilegeToRole(header, grantRevokePrivParams, resp);
        break;
      case USER:
        authzManager_.grantPrivilegeToUser(header, grantRevokePrivParams,
            resp);
        break;
      case GROUP:
        authzManager_.grantPrivilegeToGroup(header, grantRevokePrivParams,
            resp);
        break;
      default:
        throw new IllegalArgumentException("Unexpected principal type: " +
            grantRevokePrivParams.principal_type);
    }

    addSummary(resp, "Privilege(s) have been granted.");
  }

  /**
   * Revokes one or more privileges to role on behalf of the requestingUser.
   */
  private void revokePrivilege(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams grantRevokePrivParams, TDdlExecResponse resp)
      throws ImpalaException {
    Preconditions.checkNotNull(header);
    Preconditions.checkNotNull(grantRevokePrivParams);
    Preconditions.checkNotNull(resp);
    Preconditions.checkArgument(!grantRevokePrivParams.isIs_grant());

    switch (grantRevokePrivParams.principal_type) {
      case ROLE:
        authzManager_.revokePrivilegeFromRole(header, grantRevokePrivParams, resp);
        break;
      case USER:
        authzManager_.revokePrivilegeFromUser(header, grantRevokePrivParams, resp);
        break;
      case GROUP:
        authzManager_.revokePrivilegeFromGroup(header, grantRevokePrivParams, resp);
        break;
      default:
        throw new IllegalArgumentException("Unexpected principal type: " +
            grantRevokePrivParams.principal_type);
    }

    addSummary(resp, "Privilege(s) have been revoked.");
  }

  /**
   * Returns the metastore.api.Table object from the Hive Metastore for an existing
   * fully loaded table.
   */
  private org.apache.hadoop.hive.metastore.api.Table getMetaStoreTable(
      MetaStoreClient msClient, Table tbl) throws CatalogException {
    Preconditions.checkState(!(tbl instanceof IncompleteTable));
    Preconditions.checkNotNull(msClient);
    Db db = tbl.getDb();
    org.apache.hadoop.hive.metastore.api.Table msTbl = null;
    Stopwatch hmsLoadSW = Stopwatch.createStarted();
    long hmsLoadTime;
    try {
      msTbl = msClient.getHiveClient().getTable(db.getName(), tbl.getName());
    } catch (Exception e) {
      throw new TableLoadingException("Error loading metadata for table: " +
          db.getName() + "." + tbl.getName(), e);
    } finally {
      hmsLoadTime = hmsLoadSW.elapsed(TimeUnit.NANOSECONDS);
    }
    tbl.updateHMSLoadTableSchemaTime(hmsLoadTime);
    return msTbl;
  }

  /**
   * Returns the metastore.api.Table object from the Hive Metastore for an existing
   * fully loaded table. Gets the MetaStore object from 'catalog_'.
   */
  private org.apache.hadoop.hive.metastore.api.Table getTableFromMetaStore(
      TableName tblName) throws CatalogException {
    Preconditions.checkNotNull(tblName);
    org.apache.hadoop.hive.metastore.api.Table msTbl = null;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msTbl = msClient.getHiveClient().getTable(tblName.getDb(),tblName.getTbl());
    } catch (TException e) {
      LOG.error(String.format(HMS_RPC_ERROR_FORMAT_STR, "getTable") + e.getMessage());
    }
    return msTbl;
  }


  /**
   * Executes a TResetMetadataRequest and returns the result as a
   * TResetMetadataResponse. Based on the request parameters, this operation
   * may do one of three things:
   * 1) invalidate the entire catalog, forcing the metadata for all catalog
   *    objects to be reloaded.
   * 2) invalidate a specific table, forcing the metadata to be reloaded
   *    on the next access.
   * 3) perform a synchronous incremental refresh of a specific table.
   * 4) perform a refresh on authorization metadata.
   *
   * For details on the specific commands see comments on their respective
   * methods in CatalogServiceCatalog.java.
   */
  public TResetMetadataResponse execResetMetadata(TResetMetadataRequest req)
      throws CatalogException {
    String cmdString = String.format("%s issued by %s",
        req.is_refresh ? "REFRESH":"INVALIDATE",
        req.header != null ? req.header.requesting_user : " unknown user");
    TResetMetadataResponse resp = new TResetMetadataResponse();
    resp.setResult(new TCatalogUpdateResult());
    resp.getResult().setCatalog_service_id(JniCatalog.getServiceId());

    if (req.isSetDb_name()) {
      Preconditions.checkState(!catalog_.isBlacklistedDb(req.getDb_name()),
          String.format("Can't refresh functions in blacklisted database: %s. %s",
              req.getDb_name(), BLACKLISTED_DBS_INCONSISTENT_ERR_STR));
      // This is a "refresh functions" operation.
      synchronized (metastoreDdlLock_) {
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
          List<TCatalogObject> addedFuncs = Lists.newArrayList();
          List<TCatalogObject> removedFuncs = Lists.newArrayList();
          catalog_.refreshFunctions(msClient, req.getDb_name(), addedFuncs, removedFuncs);
          resp.result.setUpdated_catalog_objects(addedFuncs);
          resp.result.setRemoved_catalog_objects(removedFuncs);
          resp.result.setVersion(catalog_.getCatalogVersion());
          for (TCatalogObject removedFn: removedFuncs) {
            catalog_.getDeleteLog().addRemovedObject(removedFn);
          }
        }
      }
    } else if (req.isSetTable_name()) {
      // Results of an invalidate operation, indicating whether the table was removed
      // from the Metastore, and whether a new database was added to Impala as a result
      // of the invalidate operation. Always false for refresh.
      Reference<Boolean> tblWasRemoved = new Reference<Boolean>(false);
      Reference<Boolean> dbWasAdded = new Reference<Boolean>(false);
      // Thrift representation of the result of the invalidate/refresh operation.
      TCatalogObject updatedThriftTable = null;
      if (req.isIs_refresh()) {
        TableName tblName = TableName.fromThrift(req.getTable_name());
        // Quick check to see if the table exists in the catalog without triggering
        // a table load.
        Table tbl = catalog_.getTable(tblName.getDb(), tblName.getTbl());
        if (tbl != null) {
          // If the table is not loaded, no need to perform refresh after the initial
          // metadata load.
          boolean isTableLoadedInCatalog = tbl.isLoaded();
          tbl = getExistingTable(tblName.getDb(), tblName.getTbl(),
              "Load triggered by " + cmdString);
          CatalogObject.ThriftObjectType resultType =
              req.header.want_minimal_response ?
                  CatalogObject.ThriftObjectType.INVALIDATION :
                  CatalogObject.ThriftObjectType.FULL;
          if (isTableLoadedInCatalog) {
            if (req.isSetPartition_spec()) {
              boolean isTransactional = AcidUtils.isTransactionalTable(
                  tbl.getMetaStoreTable().getParameters());
              Preconditions.checkArgument(!isTransactional);
              Reference<Boolean> wasPartitionRefreshed = new Reference<>(false);
              // TODO if the partition was not really refreshed because the partSpec
              // was wrong, do we still need to send back the table?
              updatedThriftTable = catalog_.reloadPartition(tbl,
                  req.getPartition_spec(), wasPartitionRefreshed, resultType, cmdString);
            } else {
              // TODO IMPALA-8809: Optimisation for partitioned tables:
              //   1: Reload the whole table if schema change happened. Identify
              //     such scenario by checking Table.TBL_PROP_LAST_DDL_TIME property.
              //     Note, table level writeId is not updated by HMS for partitioned
              //     ACID tables, there is a Jira to cover this: HIVE-22062.
              //   2: If no need for a full table reload then fetch partition level
              //     writeIds and reload only the ones that changed.
              updatedThriftTable = catalog_.reloadTable(tbl, req, resultType, cmdString);
            }
          } else {
            // Table was loaded from scratch, so it's already "refreshed".
            tbl.takeReadLock();
            try {
              updatedThriftTable = tbl.toTCatalogObject(resultType);
            } finally {
              tbl.releaseReadLock();
            }
          }
        }
      } else {
        updatedThriftTable = catalog_.invalidateTable(
            req.getTable_name(), tblWasRemoved, dbWasAdded);
      }

      if (updatedThriftTable == null) {
        // Table does not exist in the Metastore and Impala catalog, throw error.
        throw new TableNotFoundException("Table not found: " +
            req.getTable_name().getDb_name() + "." +
            req.getTable_name().getTable_name());
      }

      // Return the TCatalogObject in the result to indicate this request can be
      // processed as a direct DDL operation.
      if (tblWasRemoved.getRef()) {
        resp.getResult().addToRemoved_catalog_objects(updatedThriftTable);
      } else {
        // TODO(IMPALA-9937): if client is a 'v1' impalad, only send back incremental
        //  updates
        resp.getResult().addToUpdated_catalog_objects(updatedThriftTable);
      }

      if (dbWasAdded.getRef()) {
        Db addedDb = catalog_.getDb(updatedThriftTable.getTable().getDb_name());
        if (addedDb == null) {
          throw new CatalogException("Database " +
              updatedThriftTable.getTable().getDb_name() + " was removed by a " +
              "concurrent operation. Try invalidating the table again.");
        }
        resp.getResult().addToUpdated_catalog_objects(addedDb.toTCatalogObject());
      }
      resp.getResult().setVersion(updatedThriftTable.getCatalog_version());
    } else if (req.isAuthorization()) {
      AuthorizationDelta authzDelta = catalog_.refreshAuthorization(false);
      resp.result.setUpdated_catalog_objects(authzDelta.getCatalogObjectsAdded());
      resp.result.setRemoved_catalog_objects(authzDelta.getCatalogObjectsRemoved());
      resp.result.setVersion(catalog_.getCatalogVersion());
    } else {
      // Invalidate the entire catalog if no table name is provided.
      Preconditions.checkArgument(!req.isIs_refresh());
      resp.getResult().setVersion(catalog_.reset());
      resp.getResult().setIs_invalidate(true);
    }
    if (req.isSync_ddl()) {
      resp.getResult().setVersion(catalog_.waitForSyncDdlVersion(resp.getResult()));
    }
    resp.getResult().setStatus(new TStatus(TErrorCode.OK, new ArrayList<String>()));
    return resp;
  }

  /**
   * Create any new partitions required as a result of an INSERT statement and refreshes
   * the table metadata after every INSERT statement. Any new partitions will inherit
   * their cache configuration from the parent table. That is, if the parent is cached
   * new partitions created will also be cached and will be put in the same pool as the
   * parent.
   * If the insert touched any pre-existing partitions that were cached, a request to
   * watch the associated cache directives will be submitted. This will result in an
   * async table refresh once the cache request completes.
   */
  public TUpdateCatalogResponse updateCatalog(TUpdateCatalogRequest update)
      throws ImpalaException {
    TUpdateCatalogResponse response = new TUpdateCatalogResponse();
    // Only update metastore for Hdfs tables.
    Table table = getExistingTable(update.getDb_name(), update.getTarget_table(),
        "Load for INSERT");
    if (!(table instanceof FeFsTable)) {
      throw new InternalException("Unexpected table type: " +
          update.getTarget_table());
    }

    tryWriteLock(table, "updating the catalog");
    final Timer.Context context
        = table.getMetrics().getTimer(HdfsTable.CATALOG_UPDATE_DURATION_METRIC).time();

    long transactionId = -1;
    TblTransaction tblTxn = null;
    if (update.isSetTransaction_id()) {
      transactionId = update.getTransaction_id();
      Preconditions.checkState(transactionId > 0);
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
         // Setup transactional parameters needed to do alter table/partitions later.
         // TODO: Could be optimized to possibly save some RPCs, as these parameters are
         //       not always needed + the writeId of the INSERT could be probably reused.
         tblTxn = MetastoreShim.createTblTransaction(
             msClient.getHiveClient(), table.getMetaStoreTable(), transactionId);
      }
    }

    try {
      // Get new catalog version for table in insert.
      long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
      catalog_.getLock().writeLock().unlock();
      // Collects the cache directive IDs of any cached table/partitions that were
      // targeted. A watch on these cache directives is submitted to the
      // TableLoadingMgr and the table will be refreshed asynchronously after all
      // cache directives complete.
      List<Long> cacheDirIds = Lists.<Long>newArrayList();

      // If the table is cached, get its cache pool name and replication factor. New
      // partitions will inherit this property.
      Pair<String, Short> cacheInfo = table.getTableCacheInfo(cacheDirIds);
      String cachePoolName = cacheInfo.first;
      Short cacheReplication = cacheInfo.second;

      TableName tblName = new TableName(table.getDb().getName(), table.getName());
      List<String> errorMessages = Lists.newArrayList();
      HashSet<String> partsToLoadMetadata = null;
      Collection<? extends FeFsPartition> parts =
          FeCatalogUtils.loadAllPartitions((FeFsTable) table);
      List<FeFsPartition> affectedExistingPartitions = new ArrayList<>();
      List<org.apache.hadoop.hive.metastore.api.Partition> hmsPartitionsStatsUnset =
          Lists.newArrayList();
      addCatalogServiceIdentifiers(table, catalog_.getCatalogServiceId(),
          newCatalogVersion);
      if (table.getNumClusteringCols() > 0) {
        // Set of all partition names targeted by the insert that need to be created
        // in the Metastore (partitions that do not currently exist in the catalog).
        // In the BE, we don't currently distinguish between which targeted partitions
        // are new and which already exist, so initialize the set with all targeted
        // partition names and remove the ones that are found to exist.
        HashSet<String> partsToCreate =
            Sets.newHashSet(update.getCreated_partitions());
        partsToLoadMetadata = Sets.newHashSet(partsToCreate);
        for (FeFsPartition partition: parts) {
          String partName = partition.getPartitionName();
          // Attempt to remove this partition name from partsToCreate. If remove
          // returns true, it indicates the partition already exists.
          if (partsToCreate.remove(partName)) {
            affectedExistingPartitions.add(partition);
            // For existing partitions, we need to unset column_stats_accurate to
            // tell hive the statistics is not accurate any longer.
            if (partition.getParameters() != null &&  partition.getParameters()
                .containsKey(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
              org.apache.hadoop.hive.metastore.api.Partition hmsPartition =
                  ((HdfsPartition) partition).toHmsPartition();
              hmsPartition.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
              hmsPartitionsStatsUnset.add(hmsPartition);
            }
            if (partition.isMarkedCached()) {
              // The partition was targeted by the insert and is also cached. Since
              // data was written to the partition, a watch needs to be placed on the
              // cache directive so the TableLoadingMgr can perform an async
              // refresh once all data becomes cached.
              cacheDirIds.add(HdfsCachingUtil.getCacheDirectiveId(
                  partition.getParameters()));
            }
          }
          if (partsToCreate.size() == 0) break;
        }

        if (!partsToCreate.isEmpty()) {
          try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
            org.apache.hadoop.hive.metastore.api.Table msTbl =
                table.getMetaStoreTable().deepCopy();
            List<org.apache.hadoop.hive.metastore.api.Partition> hmsParts =
                Lists.newArrayList();
            HiveConf hiveConf = new HiveConf(this.getClass());
            Warehouse warehouse = new Warehouse(hiveConf);
            for (String partName: partsToCreate) {
              org.apache.hadoop.hive.metastore.api.Partition partition =
                  new org.apache.hadoop.hive.metastore.api.Partition();
              hmsParts.add(partition);

              partition.setDbName(tblName.getDb());
              partition.setTableName(tblName.getTbl());
              partition.setValues(MetaStoreUtil.getPartValsFromName(msTbl, partName));
              partition.setParameters(new HashMap<String, String>());
              partition.setSd(msTbl.getSd().deepCopy());
              partition.getSd().setSerdeInfo(msTbl.getSd().getSerdeInfo().deepCopy());
              partition.getSd().setLocation(msTbl.getSd().getLocation() + "/" + partName);
              addCatalogServiceIdentifiers(msTbl, partition);
              MetastoreShim.updatePartitionStatsFast(partition, msTbl, warehouse);
            }

            // First add_partitions and then alter_partitions the successful ones with
            // caching directives. The reason is that some partitions could have been
            // added concurrently, and we want to avoid caching a partition twice and
            // leaking a caching directive.
            List<org.apache.hadoop.hive.metastore.api.Partition> addedHmsParts =
                msClient.getHiveClient().add_partitions(hmsParts, true, true);

            if (addedHmsParts.size() > 0) {
              if (cachePoolName != null) {
                List<org.apache.hadoop.hive.metastore.api.Partition> cachedHmsParts =
                    Lists.newArrayList();
                // Submit a new cache directive and update the partition metadata with
                // the directive id.
                for (org.apache.hadoop.hive.metastore.api.Partition part: addedHmsParts) {
                  try {
                    cacheDirIds.add(HdfsCachingUtil.submitCachePartitionDirective(
                        part, cachePoolName, cacheReplication));
                    StatsSetupConst.setBasicStatsState(part.getParameters(), "false");
                    cachedHmsParts.add(part);
                  } catch (ImpalaRuntimeException e) {
                    String msg = String.format("Partition %s.%s(%s): State: Not " +
                        "cached. Action: Cache manully via 'ALTER TABLE'.",
                        part.getDbName(), part.getTableName(), part.getValues());
                    LOG.error(msg, e);
                    errorMessages.add(msg);
                  }
                }
                try {
                  MetastoreShim.alterPartitions(msClient.getHiveClient(), tblName.getDb(),
                      tblName.getTbl(), cachedHmsParts);
                } catch (Exception e) {
                  LOG.error("Failed in alter_partitions: ", e);
                  // Try to uncache the partitions when the alteration in the HMS
                  // failed.
                  for (org.apache.hadoop.hive.metastore.api.Partition part:
                      cachedHmsParts) {
                    try {
                      HdfsCachingUtil.removePartitionCacheDirective(part.getParameters());
                    } catch (ImpalaException e1) {
                      String msg = String.format(
                          "Partition %s.%s(%s): State: Leaked caching directive. " +
                          "Action: Manually uncache directory %s via hdfs " +
                          "cacheAdmin.", part.getDbName(), part.getTableName(),
                          part.getValues(), part.getSd().getLocation());
                      LOG.error(msg, e);
                      errorMessages.add(msg);
                    }
                  }
                }
              }
            }
          } catch (AlreadyExistsException e) {
            throw new InternalException(
                "AlreadyExistsException thrown although ifNotExists given", e);
          } catch (Exception e) {
            throw new InternalException("Error adding partitions", e);
          }
        }

        // Unset COLUMN_STATS_ACCURATE by calling alter partition to hms.
        if (!hmsPartitionsStatsUnset.isEmpty()) {
          unsetPartitionsColStats(table.getMetaStoreTable(), hmsPartitionsStatsUnset,
              tblTxn);
        }
      } else {
        // For non-partitioned table, only single part exists
        FeFsPartition singlePart = Iterables.getOnlyElement((List<FeFsPartition>) parts);
        affectedExistingPartitions.add(singlePart);
      }
      unsetTableColStats(table.getMetaStoreTable(), tblTxn);
      // Submit the watch request for the given cache directives.
      if (!cacheDirIds.isEmpty()) {
        catalog_.watchCacheDirs(cacheDirIds, tblName.toThrift(),
            "INSERT into cached partitions");
      }

      response.setResult(new TCatalogUpdateResult());
      response.getResult().setCatalog_service_id(JniCatalog.getServiceId());
      if (errorMessages.size() > 0) {
        errorMessages.add("Please refer to the catalogd error log for details " +
            "regarding the failed un/caching operations.");
        response.getResult().setStatus(
            new TStatus(TErrorCode.INTERNAL_ERROR, errorMessages));
      } else {
        response.getResult().setStatus(
            new TStatus(TErrorCode.OK, new ArrayList<String>()));
      }

      // Commit transactional inserts on success. We don't abort the transaction
      // here in case of failures, because the client, i.e. query coordinator, is
      // always responsible for aborting transactions when queries hit errors.
      boolean skipTransactionalInsertEvent = false;
      if (update.isSetTransaction_id()) {
        if (response.getResult().getStatus().getStatus_code() == TErrorCode.OK) {
          commitTransaction(update.getTransaction_id());
        } else {
          // do not generate the insert event if the transaction was aborted.
          skipTransactionalInsertEvent = true;
        }
      }

      if (table instanceof FeIcebergTable && update.isSetIceberg_operation()) {
        IcebergCatalogOpExecutor.appendFiles((FeIcebergTable)table,
            update.getIceberg_operation());
      }

      // the load operation below changes the partitions in-place (this was later
      // changed in upstream). If the partitions are loaded in-place, we cannot keep track
      // of files before the load and hence the firing of insert events later below
      // cannot find the new files.
      Map<String, Set<String>> filesBeforeInsert = Maps.newHashMap();
      if (shouldGenerateInsertEvents()) {
        filesBeforeInsert = getPartitionNameFilesMap(affectedExistingPartitions);
      }
      loadTableMetadata(table, newCatalogVersion, true, false, partsToLoadMetadata,
          "INSERT");
      // After loading metadata, fire insert events if external event processing is
      // enabled.
      if (!skipTransactionalInsertEvent) {
        // new parts are only created in case the table is partitioned.
        Set<String> newPartsCreated = table.getNumClusteringCols() > 0 ? Sets
            .difference(Preconditions.checkNotNull(partsToLoadMetadata),
                filesBeforeInsert.keySet()) : Sets.newHashSetWithExpectedSize(0);
        long txnId = tblTxn == null ? -1 : tblTxn.txnId;
        long writeId = tblTxn == null ? -1: tblTxn.writeId;
        createInsertEvents(table, filesBeforeInsert, affectedExistingPartitions,
            newPartsCreated, update.is_overwrite, txnId, writeId);
      }
      addTableToCatalogUpdate(table, update.header.want_minimal_response,
          response.result);
    } finally {
      context.stop();
      UnlockWriteLockIfErronouslyLocked();
      table.releaseWriteLock();
    }

    if (update.isSync_ddl()) {
      response.getResult().setVersion(
          catalog_.waitForSyncDdlVersion(response.getResult()));
    }
    return response;
  }

  /**
   * Populates insert event data and calls fireInsertEvents() if external event
   * processing is enabled. This is no-op if event processing is disabled or there are
   * no existing partitions affected by this insert.
   * @param partitionFilesMapBeforeInsert List of files for each partition in which data
   *                                     was inserted.
   * @param affectedExistingPartitions List of all affected partitions that were already
   *                                   existed.
   * @param newPartsCreated represents all the partitions names which got created by
   *                       the insert operation.
   * @param isInsertOverwrite indicates if the operation was an insert overwrite.
   */
  private void createInsertEvents(Table table,
      Map<String, Set<String>> partitionFilesMapBeforeInsert,
      List<FeFsPartition> affectedExistingPartitions,
      Set<String> newPartsCreated, boolean isInsertOverwrite,
      long txnId, long writeId) throws CatalogException {
    if (!shouldGenerateInsertEvents()) {
      return;
    }

    // List of all insert events that we call HMS fireInsertEvent() on.
    List<InsertEventRequestData> insertEventReqDatas = new ArrayList<>();
    // List of all partitions that we insert into
    List<HdfsPartition> partitions = new ArrayList<>();
    Collection<? extends FeFsPartition> partsPostInsert;
    if (table.getNumClusteringCols() > 0) {
      partsPostInsert = ((HdfsTable) table).getPartitionsForNames(
          partitionFilesMapBeforeInsert.keySet());
    } else {
      partsPostInsert = FeCatalogUtils.loadAllPartitions((FeFsTable) table);
    }

    Map<String, Set<String>> partitionFilesMapPostInsert = getPartitionNameFilesMap(
        partsPostInsert);

    for (FeFsPartition part : partsPostInsert) {
      // Find the delta of the files added by the insert if it is not an overwrite
      // operation. HMS fireListenerEvent() expects an empty list if no new files are
      // added or if the operation is an insert overwrite.
      HdfsPartition hdfsPartition = (HdfsPartition) part;
      // newFiles keeps track of newly added files by this insert operation.
      Set<String> newFiles;
      List<String> partVals = null;
      String partitionName = hdfsPartition.getPartitionName();
      Set<String> filesPostInsert =
          partitionFilesMapPostInsert.get(partitionName);
      Set<String> filesBeforeInsert =
          partitionFilesMapBeforeInsert.get(partitionName);
      newFiles = isInsertOverwrite ? filesPostInsert
          : Sets.difference(filesPostInsert, filesBeforeInsert);
      // if the table is unpartitioned partVals will be empty
      partVals = hdfsPartition.getPartitionValuesAsStrings(true);
      LOG.info(String.format("%s new files detected for table %s%s", newFiles.size(),
          table.getFullName(),
          ((FeFsTable) table).isPartitioned() ? " partition " + hdfsPartition
              .getPartitionName() : ""));
      if (!newFiles.isEmpty()) {
        // Collect all the insert events.
        insertEventReqDatas.add(
            makeInsertEventData((FeFsTable) table, partVals, newFiles,
                isInsertOverwrite));
        if (!partVals.isEmpty()) {
          // insert into partition
          partitions.add(hdfsPartition);
        }
      }
    }

    // if the table is transaction table we should generate a transactional
    // insert event type.
    boolean isTransactional = AcidUtils.isTransactionalTable(table.getMetaStoreTable()
        .getParameters());
    Preconditions.checkState(!isTransactional || txnId > 0, String
        .format("Invalid transaction id %s for generating insert events on table %s",
            txnId, table.getFullName()));
    Preconditions.checkState(!isTransactional || writeId > 0,
        String.format("Invalid write id %s for generating insert events on table %s",
            writeId, table.getFullName()));
    // if the table is transactional we fire the event for new partitions as well.
    if (isTransactional) {
      for (HdfsPartition newPart : ((HdfsTable) table)
          .getPartitionsForNames(newPartsCreated)) {
        insertEventReqDatas.add(makeInsertEventData((HdfsTable) table,
            newPart.getPartitionValuesAsStrings(true), newPart.getFileNames(),
            isInsertOverwrite));
      }
    }

    if (insertEventReqDatas.isEmpty()) return;

    MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient();
    TableInsertEventInfo insertEventInfo = new TableInsertEventInfo(
        insertEventReqDatas, isTransactional, txnId, writeId);
    List<Long> eventIds = MetastoreShim.fireInsertEvents(metaStoreClient,
        insertEventInfo, table.getDb().getName(), table.getName());
    if (!eventIds.isEmpty()) {
      if (partitions.size() == 0) { // insert into table
        catalog_.addVersionsForInflightEvents(true, table, eventIds.get(0));
      } else { // insert into partition
        for (int par_idx = 0; par_idx < partitions.size(); par_idx++) {
          partitions.get(par_idx).addToVersionsForInflightEvents(
              true, eventIds.get(par_idx));
        }
      }
    }
  }

  private boolean shouldGenerateInsertEvents() {
    return catalog_.isEventProcessingActive() && BackendConfig.INSTANCE
        .isInsertEventsEnabled();
  }

  private InsertEventRequestData makeInsertEventData(FeFsTable tbl, List<String> partVals,
      Set<String> newFiles, boolean isInsertOverwrite) throws CatalogException {
    Preconditions.checkNotNull(newFiles);
    Preconditions.checkNotNull(partVals);
    InsertEventRequestData insertEventRequestData = new InsertEventRequestData(
        Lists.newArrayListWithCapacity(
            newFiles.size()));
    boolean isTransactional = AcidUtils
        .isTransactionalTable(tbl.getMetaStoreTable().getParameters());
    // in case of unpartitioned table, partVals will be empty
    if (!partVals.isEmpty()) insertEventRequestData.setPartitionVal(partVals);
    FileSystem fs = tbl.getFileSystem();
    for (String file : newFiles) {
      try {
        Path filePath = new Path(file);
        FileChecksum checkSum = fs.getFileChecksum(filePath);
        String checksumStr = checkSum == null ? ""
            : StringUtils.byteToHexString(checkSum.getBytes(), 0, checkSum.getLength());
        insertEventRequestData.addToFilesAdded(file);
        insertEventRequestData.addToFilesAddedChecksum(checksumStr);
        if (isTransactional) {
          String acidDirPath = AcidUtils.getFirstLevelAcidDirPath(filePath, fs);
          if (acidDirPath != null) {
            insertEventRequestData.addToSubDirectoryList(acidDirPath);
          }
        }
        insertEventRequestData.setReplace(isInsertOverwrite);
      } catch (IOException e) {
        if (tbl instanceof FeIcebergTable) {
          // TODO IMPALA-10254: load data files via Iceberg API. Currently we load
          // Iceberg data files via file listing, so we might see files being written.
          continue;
        }
        throw new CatalogException("Could not get the file checksum for file " + file, e);
      }
    }
    return insertEventRequestData;
  }

  /**
   * Util method to return a map of partition names to list of files for that partition.
   */
  private static Map<String, Set<String>> getPartitionNameFilesMap(Collection<?
      extends FeFsPartition> partitions) {
    Map<String, Set<String>> partitionFilePaths = new HashMap<>();
    for (FeFsPartition partition : partitions) {
      String key = partition.getPartitionName();
      Set<String> filenames = ((HdfsPartition) partition).getFileNames();
      partitionFilePaths.putIfAbsent(key, new HashSet<>(filenames.size()));
      partitionFilePaths.get(key).addAll(filenames);
    }
    return partitionFilePaths;
  }

  /**
   * Returns an existing, loaded table from the Catalog. Throws an exception if any
   * of the following are true:
   * - The table does not exist
   * - There was an error loading the table metadata.
   * - The table is missing (not yet loaded).
   * This is to help protect against certain scenarios where the table was
   * modified or dropped between the time analysis completed and the the catalog op
   * started executing. However, even with these checks it is possible the table was
   * modified or dropped/re-created without us knowing. This function also updates the
   * table usage counter.
   *
   * TODO: Track object IDs to
   * know when a table has been dropped and re-created with the same name.
   */
  public Table getExistingTable(String dbName, String tblName, String reason)
      throws CatalogException {
    // passing null validWriteIdList makes sure that we return the table if it is
    // already loaded.
    Table tbl = catalog_.getOrLoadTable(dbName, tblName, reason, null);
    if (tbl == null) {
      throw new TableNotFoundException("Table not found: " + dbName + "." + tblName);
    }
    tbl.incrementMetadataOpsCount();

    if (!tbl.isLoaded()) {
      throw new CatalogException(String.format("Table '%s.%s' was modified while " +
          "operation was in progress, aborting execution.", dbName, tblName));
    }

    if (tbl instanceof IncompleteTable && tbl.isLoaded()) {
      // The table loading failed. Throw an exception.
      ImpalaException e = ((IncompleteTable) tbl).getCause();
      if (e instanceof TableLoadingException) {
        throw (TableLoadingException) e;
      }
      throw new TableLoadingException(e.getMessage(), e);
    }
    Preconditions.checkNotNull(tbl);
    Preconditions.checkState(tbl.isLoaded());
    return tbl;
  }

  private void alterCommentOn(TCommentOnParams params, TDdlExecResponse response,
      Optional<TTableName> tTableName, boolean wantMinimalResult)
      throws ImpalaRuntimeException, CatalogException, InternalException {
    if (params.getDb() != null) {
      Preconditions.checkArgument(!params.isSetTable_name() &&
          !params.isSetColumn_name());
      tTableName.get().setDb_name(params.db);
      catalogOpMetric_.increment(TDdlType.COMMENT_ON, tTableName);
      alterCommentOnDb(params.getDb(), params.getComment(), wantMinimalResult, response);
    } else if (params.getTable_name() != null) {
      Preconditions.checkArgument(!params.isSetDb() && !params.isSetColumn_name());
      tTableName.get().setDb_name(params.table_name.db_name);
      tTableName.get().setTable_name(params.table_name.table_name);
      catalogOpMetric_.increment(TDdlType.COMMENT_ON, tTableName);
      alterCommentOnTableOrView(TableName.fromThrift(params.getTable_name()),
          params.getComment(), wantMinimalResult, response);
    } else if (params.getColumn_name() != null) {
      Preconditions.checkArgument(!params.isSetDb() && !params.isSetTable_name());
      TColumnName columnName = params.getColumn_name();
      tTableName.get().setDb_name(columnName.table_name.table_name);
      tTableName.get().setTable_name(columnName.table_name.table_name);
      catalogOpMetric_.increment(TDdlType.COMMENT_ON, tTableName);
      alterCommentOnColumn(TableName.fromThrift(columnName.getTable_name()),
          columnName.getColumn_name(), params.getComment(), wantMinimalResult, response);
    } else {
      throw new UnsupportedOperationException("Unsupported COMMENT ON operation");
    }
  }

  private void alterCommentOnDb(String dbName, String comment, boolean wantMinimalResult,
      TDdlExecResponse response)
      throws ImpalaRuntimeException, CatalogException, InternalException {
    Db db = catalog_.getDb(dbName);
    if (db == null) {
      throw new CatalogException("Database: " + dbName + " does not exist.");
    }
    tryLock(db, "altering the comment");
    // Get a new catalog version to assign to the database being altered.
    long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
    try {
      Database msDb = db.getMetaStoreDb().deepCopy();
      addCatalogServiceIdentifiers(msDb, catalog_.getCatalogServiceId(),
          newCatalogVersion);
      msDb.setDescription(comment);
      try {
        applyAlterDatabase(msDb);
      } catch (ImpalaRuntimeException e) {
        throw e;
      }
      Db updatedDb = catalog_.updateDb(msDb);
      addDbToCatalogUpdate(updatedDb, wantMinimalResult, response.result);
      // now that HMS alter operation has succeeded, add this version to list of inflight
      // events in catalog database if event processing is enabled
      catalog_.addVersionsForInflightEvents(db, newCatalogVersion);
    } finally {
      db.getLock().unlock();
    }
    addSummary(response, "Updated database.");
  }


  private void alterCommentOnTableOrView(TableName tableName, String comment,
      boolean wantMinimalResult, TDdlExecResponse response)
      throws CatalogException, InternalException, ImpalaRuntimeException {
    Table tbl = getExistingTable(tableName.getDb(), tableName.getTbl(),
        "Load for ALTER COMMENT");
    tryWriteLock(tbl);
    try {
      long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
      catalog_.getLock().writeLock().unlock();
      addCatalogServiceIdentifiers(tbl, catalog_.getCatalogServiceId(),
          newCatalogVersion);
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      boolean isView = msTbl.getTableType().equalsIgnoreCase(
          TableType.VIRTUAL_VIEW.toString());
      if (comment == null) {
        msTbl.getParameters().remove("comment");
      } else {
        msTbl.getParameters().put("comment", comment);
      }
      applyAlterTable(msTbl);
      loadTableMetadata(tbl, newCatalogVersion, false, false, null, "ALTER COMMENT");
      addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
      addSummary(response, String.format("Updated %s.", (isView) ? "view" : "table"));
    } finally {
      tbl.releaseWriteLock();
    }
  }

  private void alterCommentOnColumn(TableName tableName, String columnName,
      String comment, boolean wantMinimalResult, TDdlExecResponse response)
      throws CatalogException, InternalException, ImpalaRuntimeException {
    Table tbl = getExistingTable(tableName.getDb(), tableName.getTbl(),
        "Load for ALTER COLUMN COMMENT");
    tryWriteLock(tbl);
    try {
      long newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
      catalog_.getLock().writeLock().unlock();
      if (tbl instanceof KuduTable) {
        TColumn new_col = new TColumn(columnName,
            tbl.getColumn(columnName).getType().toThrift());
        new_col.setComment(comment != null ? comment : "");
        KuduCatalogOpExecutor.alterColumn((KuduTable) tbl, columnName, new_col);
      } else {
        org.apache.hadoop.hive.metastore.api.Table msTbl =
            tbl.getMetaStoreTable().deepCopy();
        if (!updateColumnComment(msTbl.getSd().getColsIterator(), columnName, comment)) {
          if (!updateColumnComment(msTbl.getPartitionKeysIterator(), columnName,
              comment)) {
            throw new ColumnNotFoundException(String.format(
                "Column name %s not found in table %s.", columnName, tbl.getFullName()));
          }
        }
        applyAlterTable(msTbl);
      }
      loadTableMetadata(tbl, newCatalogVersion, false, true, null,
          "ALTER COLUMN COMMENT");
      addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
      addSummary(response, "Column has been altered.");
    } finally {
      tbl.releaseWriteLock();
    }
  }

  /**
   * Find the matching column name in the iterator and update its comment. Return
   * true if found; false otherwise.
   */
  private static boolean updateColumnComment(Iterator<FieldSchema> iterator,
      String columnName, String comment) {
    while (iterator.hasNext()) {
      FieldSchema fs = iterator.next();
      if (fs.getName().equalsIgnoreCase(columnName)) {
        fs.setComment(comment);
        return true;
      }
    }
    return false;
  }

  /**
   * Tries to take the write lock of the table in the catalog. Throws an InternalException
   * if the catalog is unable to lock the given table.
   */
  public void tryWriteLock(Table tbl) throws InternalException {
    tryWriteLock(tbl, "altering");
  }

  /**
   * Try to lock a table in the catalog for a given operation. Throw an InternalException
   * if the catalog is unable to lock the given table.
   */
  public void tryWriteLock(Table tbl, String operation) throws InternalException {
    String type = tbl instanceof View ? "view" : "table";
    if (!catalog_.tryWriteLock(tbl)) {
      throw new InternalException(String.format("Error %s (for) %s %s due to " +
          "lock contention.", operation, type, tbl.getFullName()));
    }
  }

  /**
   * Try to lock the given Db in the catalog for the given operation. Throws
   * InternalException if catalog is unable to lock the database.
   */
  public void tryLock(Db db, String operation) throws InternalException {
    if (!catalog_.tryLockDb(db)) {
      throw new InternalException(String.format("Error %s of database %s due to lock "
          + "contention.", operation, db.getName()));
    }
  }

  /**
   * Commits ACID transaction with given transaction id.
   * @param transactionId is the id of the transaction.
   * @throws TransactionException
   */
  private void commitTransaction(long transactionId) throws TransactionException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      MetastoreShim.commitTransaction(msClient.getHiveClient(), transactionId);
      LOG.info("Committed transaction: " + Long.toString(transactionId));
    }
  }

  /**
   * Update table properties to remove the COLUMN_STATS_ACCURATE entry if it exists.
   */
  private void unsetTableColStats(org.apache.hadoop.hive.metastore.api.Table msTable,
      TblTransaction tblTxn) throws ImpalaRuntimeException{
    Map<String, String> params = msTable.getParameters();
    if (params != null && params.containsKey(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
      params.remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
      applyAlterTable(msTable, false, tblTxn);
    }
  }

  /**
   * Update partitions properties to remove the COLUMN_STATS_ACCURATE entry from HMS.
   * This method assumes the partitions in the input hmsPartitionsStatsUnset already
   * had the COLUMN_STATS_ACCURATE removed from their properties.
   */
  private void unsetPartitionsColStats(org.apache.hadoop.hive.metastore.api.Table msTable,
      List<org.apache.hadoop.hive.metastore.api.Partition> hmsPartitionsStatsUnset,
      TblTransaction tblTxn) throws ImpalaRuntimeException{
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      try {
        if (tblTxn != null) {
          MetastoreShim.alterPartitionsWithTransaction(
              msClient.getHiveClient(), msTable.getDbName(), msTable.getTableName(),
              hmsPartitionsStatsUnset,  tblTxn);
        } else {
          MetastoreShim.alterPartitions(msClient.getHiveClient(), msTable.getDbName(),
              msTable.getTableName(), hmsPartitionsStatsUnset);
        }
      } catch (TException te) {
        new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_partitions"), te);
      }
    }
  }

}
