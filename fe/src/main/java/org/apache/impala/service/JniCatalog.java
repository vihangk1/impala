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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.events.ExternalEventsProcessor;
import org.apache.impala.catalog.events.MetastoreEventsProcessor;
import org.apache.impala.catalog.events.NoOpEventProcessor;
import org.apache.impala.catalog.monitor.CatalogMonitor;
import org.apache.impala.catalog.monitor.CatalogOperationMetrics;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TGetCatalogDeltaResponse;
import org.apache.impala.thrift.TGetCatalogDeltaRequest;
import org.apache.impala.thrift.TGetCatalogServerMetricsResponse;
import org.apache.impala.thrift.TGetDbsParams;
import org.apache.impala.thrift.TGetDbsResult;
import org.apache.impala.thrift.TGetFunctionsRequest;
import org.apache.impala.thrift.TGetFunctionsResponse;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartitionStatsRequest;
import org.apache.impala.thrift.TGetPartitionStatsResponse;
import org.apache.impala.thrift.TGetTablesParams;
import org.apache.impala.thrift.TGetTableMetricsParams;
import org.apache.impala.thrift.TGetTablesResult;
import org.apache.impala.thrift.TLogLevel;
import org.apache.impala.thrift.TPrioritizeLoadRequest;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUpdateCatalogRequest;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TUpdateTableUsageRequest;
import org.apache.impala.util.AuthorizationUtil;
import org.apache.impala.util.GlogAppender;
import org.apache.impala.util.PatternMatcher;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JNI-callable interface for the CatalogService. The main point is to serialize
 * and de-serialize thrift structures between C and Java parts of the CatalogService.
 */
public class JniCatalog {
  private final static Logger LOG = LoggerFactory.getLogger(JniCatalog.class);
  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();
  private final CatalogServiceCatalog catalog_;
  private final CatalogOpExecutor catalogOpExecutor_;
  private final AuthorizationManager authzManager_;

  // A unique identifier for this instance of the Catalog Service.
  private static final TUniqueId catalogServiceId_ = generateId();

  // A singleton monitoring class that keeps track of the catalog usage metrics.
  private final CatalogOperationMetrics catalogOperationUsage =
      CatalogMonitor.INSTANCE.getCatalogOperationMetrics();

  private static TUniqueId generateId() {
    UUID uuid = UUID.randomUUID();
    return new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
  }

  public JniCatalog(byte[] thriftBackendConfig) throws InternalException,
      ImpalaException, TException {
    TBackendGflags cfg = new TBackendGflags();
    JniUtil.deserializeThrift(protocolFactory_, cfg, thriftBackendConfig);

    BackendConfig.create(cfg);

    Preconditions.checkArgument(cfg.num_metadata_loading_threads > 0);
    Preconditions.checkArgument(cfg.max_hdfs_partitions_parallel_load > 0);
    Preconditions.checkArgument(cfg.max_nonhdfs_partitions_parallel_load > 0);
    Preconditions.checkArgument(cfg.initial_hms_cnxn_timeout_s > 0);
    // This trick saves having to pass a TLogLevel enum, which is an object and more
    // complex to pass through JNI.
    GlogAppender.Install(TLogLevel.values()[cfg.impala_log_lvl],
        TLogLevel.values()[cfg.non_impala_java_vlog]);

    // create the appropriate auth factory from backend config
    // this logic is shared with JniFrontend
    final AuthorizationFactory authzFactory
        = AuthorizationUtil.authzFactoryFrom(BackendConfig.INSTANCE);

    LOG.info(JniUtil.getJavaVersion());

    final AuthorizationConfig authzConfig = authzFactory.getAuthorizationConfig();

    if (MetastoreShim.getMajorVersion() > 2) {
      MetastoreShim.setHiveClientCapabilities();
    }

    MetaStoreClientPool metaStoreClientPool = new MetaStoreClientPool(
        CatalogServiceCatalog.INITIAL_META_STORE_CLIENT_POOL_SIZE,
        cfg.initial_hms_cnxn_timeout_s);
    catalog_ = new CatalogServiceCatalog(cfg.load_catalog_in_background,
        cfg.num_metadata_loading_threads, getServiceId(),
        cfg.local_library_path, metaStoreClientPool);
    authzManager_ = authzFactory.newAuthorizationManager(catalog_);
    catalog_.setAuthzManager(authzManager_);
    catalogOpExecutor_ = new CatalogOpExecutor(catalog_, authzConfig, authzManager_);
    ExternalEventsProcessor eventsProcessor = getEventsProcessor(metaStoreClientPool,
        catalogOpExecutor_);
    catalog_.setMetastoreEventProcessor(eventsProcessor);
    catalog_.startEventsProcessor();
    try {
      catalog_.reset();
    } catch (CatalogException e) {
      LOG.error("Error initializing Catalog. Please run 'invalidate metadata'", e);
    }
  }


  /**
   * Returns a Metastore event processor object if
   * <code>BackendConfig#getHMSPollingIntervalInSeconds</code> returns a non-zero
   *.value of polling interval. Otherwise, returns a no-op events processor. It is
   * important to fetch the current notification event id at the Catalog service
   * initialization time so that event processor starts to sync at the event id
   * corresponding to the catalog start time.
   */
  private ExternalEventsProcessor getEventsProcessor(
      MetaStoreClientPool metaStoreClientPool, CatalogOpExecutor catalogOpExecutor)
      throws ImpalaException {
    long eventPollingInterval = BackendConfig.INSTANCE.getHMSPollingIntervalInSeconds();
    if (eventPollingInterval <= 0) {
      LOG.info(String
          .format("Metastore event processing is disabled. Event polling interval is %d",
              eventPollingInterval));
      return NoOpEventProcessor.getInstance();
    }
    try (MetaStoreClient metaStoreClient = metaStoreClientPool.getClient()) {
      CurrentNotificationEventId currentNotificationId =
          metaStoreClient.getHiveClient().getCurrentNotificationEventId();
      return MetastoreEventsProcessor.getInstance(
          catalogOpExecutor, currentNotificationId.getEventId(), eventPollingInterval);
    } catch (TException e) {
      LOG.error("Unable to fetch the current notification event id from metastore."
          + "Metastore event processing will be disabled.", e);
      throw new CatalogException(
          "Fatal error while initializing metastore event processor", e);
    }
  }

  public static TUniqueId getServiceId() { return catalogServiceId_; }

  public byte[] getCatalogDelta(byte[] thriftGetCatalogDeltaReq) throws
      ImpalaException, TException {
    TGetCatalogDeltaRequest params = new TGetCatalogDeltaRequest();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetCatalogDeltaReq);
    return new TSerializer(protocolFactory_).serialize(new TGetCatalogDeltaResponse(
        catalog_.getCatalogDelta(params.getNative_catalog_server_ptr(),
        params.getFrom_version())));
  }

  /**
   * Gets the current catalog version.
   */
  public long getCatalogVersion() {
    return catalog_.getCatalogVersion();
  }

  /**
   * Executes the given DDL request and returns the result.
   */
  public byte[] execDdl(byte[] thriftDdlExecReq) throws ImpalaException {
    TDdlExecRequest params = new TDdlExecRequest();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftDdlExecReq);
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(catalogOpExecutor_.execDdlRequest(params));
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Execute a reset metadata statement. See comment in CatalogOpExecutor.java.
   */
  public byte[] resetMetadata(byte[] thriftResetMetadataReq)
      throws ImpalaException, TException {
    TResetMetadataRequest req = new TResetMetadataRequest();
    JniUtil.deserializeThrift(protocolFactory_, req, thriftResetMetadataReq);
    TSerializer serializer = new TSerializer(protocolFactory_);
    catalogOperationUsage.increment(req);
    try {
      return serializer.serialize(catalogOpExecutor_.execResetMetadata(req));
    } finally {
      catalogOperationUsage.decrement(req);
    }
  }

  /**
   * Returns a list of databases matching an optional pattern.
   * The argument is a serialized TGetDbParams object.
   * The return type is a serialized TGetDbResult object.
   */
  public byte[] getDbs(byte[] thriftGetTablesParams) throws ImpalaException,
      TException {
    TGetDbsParams params = new TGetDbsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetTablesParams);
    List<Db> dbs = catalog_.getDbs(PatternMatcher.MATCHER_MATCH_ALL);
    TGetDbsResult result = new TGetDbsResult();
    List<TDatabase> tDbs = Lists.newArrayListWithCapacity(dbs.size());
    for (FeDb db: dbs) tDbs.add(db.toThrift());
    result.setDbs(tDbs);
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(result);
  }

  /**
   * Returns a list of table names matching an optional pattern.
   * The argument is a serialized TGetTablesParams object.
   * The return type is a serialized TGetTablesResult object.
   */
  public byte[] getTableNames(byte[] thriftGetTablesParams) throws ImpalaException,
      TException {
    TGetTablesParams params = new TGetTablesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetTablesParams);
    List<String> tables = catalog_.getTableNames(params.db,
        PatternMatcher.createHivePatternMatcher(params.pattern));
    TGetTablesResult result = new TGetTablesResult();
    result.setTables(tables);
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(result);
  }

  /**
   * Returns the collected metrics of a table.
   */
  public String getTableMetrics(byte[] getTableMetricsParams) throws ImpalaException,
      TException {
    TGetTableMetricsParams params = new TGetTableMetricsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, getTableMetricsParams);
    return catalog_.getTableMetrics(params.table_name);
  }

  /**
   * Gets the thrift representation of a catalog object.
   */
  public byte[] getCatalogObject(byte[] thriftParams) throws ImpalaException,
      TException {
    TCatalogObject objectDescription = new TCatalogObject();
    JniUtil.deserializeThrift(protocolFactory_, objectDescription, thriftParams);
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(catalog_.getTCatalogObject(objectDescription));
  }

  /**
   * Gets the json string of a catalog object. It can only be used in showing debug
   * messages and can't be deserialized to a thrift object.
   */
  public String getJsonCatalogObject(byte[] thriftParams) throws ImpalaException,
      TException {
    TCatalogObject objectDescription = new TCatalogObject();
    JniUtil.deserializeThrift(protocolFactory_, objectDescription, thriftParams);
    TSerializer jsonSerializer = new TSerializer(new TSimpleJSONProtocol.Factory());
    return jsonSerializer.toString(catalog_.getTCatalogObject(objectDescription));
  }

  public byte[] getPartialCatalogObject(byte[] thriftParams) throws ImpalaException,
      TException {
    TGetPartialCatalogObjectRequest req =
        new TGetPartialCatalogObjectRequest();
    JniUtil.deserializeThrift(protocolFactory_, req, thriftParams);
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(catalog_.getPartialCatalogObject(req));
  }

  /**
   * See comment in CatalogServiceCatalog.
   */
  public byte[] getFunctions(byte[] thriftParams) throws ImpalaException,
      TException {
    TGetFunctionsRequest request = new TGetFunctionsRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftParams);
    TSerializer serializer = new TSerializer(protocolFactory_);
    if (!request.isSetDb_name()) {
      throw new InternalException("Database name must be set in call to " +
          "getFunctions()");
    }

    // Get all the functions and convert them to their Thrift representation.
    List<Function> fns = catalog_.getFunctions(request.getDb_name());
    TGetFunctionsResponse response = new TGetFunctionsResponse();
    response.setFunctions(new ArrayList<TFunction>(fns.size()));
    for (Function fn: fns) {
      response.addToFunctions(fn.toThrift());
    }

    return serializer.serialize(response);
  }

  public void prioritizeLoad(byte[] thriftLoadReq) throws ImpalaException,
      TException  {
    TPrioritizeLoadRequest request = new TPrioritizeLoadRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftLoadReq);
    catalog_.prioritizeLoad(request.getObject_descs());
  }

  public byte[] getPartitionStats(byte[] thriftParams)
      throws ImpalaException, TException {
    TGetPartitionStatsRequest request = new TGetPartitionStatsRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftParams);
    TSerializer serializer = new TSerializer(protocolFactory_);
    TGetPartitionStatsResponse response = new TGetPartitionStatsResponse();
    try {
      response.setPartition_stats(catalog_.getPartitionStats(request));
    } catch (CatalogException e) {
      response.setStatus(
          new TStatus(TErrorCode.INTERNAL_ERROR, ImmutableList.of(e.getMessage())));
    }
    return serializer.serialize(response);
  }

  /**
   * Process any updates to the metastore required after a query executes.
   * The argument is a serialized TCatalogUpdate.
   */
  public byte[] updateCatalog(byte[] thriftUpdateCatalog) throws ImpalaException,
      TException  {
    TUpdateCatalogRequest request = new TUpdateCatalogRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftUpdateCatalog);
    TSerializer serializer = new TSerializer(protocolFactory_);
    catalogOperationUsage.increment(request);
    try {
      return serializer.serialize(catalogOpExecutor_.updateCatalog(request));
    } finally {
      catalogOperationUsage.decrement(request);
    }
  }

  /**
   * Returns information about the current catalog usage.
   */
  public byte[] getCatalogUsage() throws ImpalaException, TException {
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(catalog_.getCatalogUsage());
  }

  /**
   * Returns information about the current catalog operation metrics.
   */
  public byte[] getOperationUsage() throws ImpalaException, TException {
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(catalog_.getOperationUsage());
  }

  public byte[] getEventProcessorSummary() throws TException {
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(catalog_.getEventProcessorSummary());
  }

  public void updateTableUsage(byte[] req) throws ImpalaException {
    TUpdateTableUsageRequest thriftReq = new TUpdateTableUsageRequest();
    JniUtil.deserializeThrift(protocolFactory_, thriftReq, req);
    catalog_.updateTableUsage(thriftReq);
  }

  public byte[] getCatalogServerMetrics() throws ImpalaException, TException {
    TGetCatalogServerMetricsResponse response = new TGetCatalogServerMetricsResponse();
    response.setCatalog_partial_fetch_rpc_queue_len(
        catalog_.getPartialFetchRpcQueueLength());
    response.setEvent_metrics(catalog_.getEventProcessorMetrics());
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(response);
  }
}
