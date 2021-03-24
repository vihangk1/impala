package org.apache.impala.catalog.operations;

import static org.apache.impala.analysis.Analyzer.ACCESSTYPE_READWRITE;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.impala.analysis.AlterTableSortByStmt;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartition.Builder;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.HiveStorageDescriptorFactory;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.RowFormat;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogUpdateResult;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.AcidUtils.TblTransaction;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CatalogOperation {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogOperation.class);
  // Format string for exceptions returned by Hive Metastore RPCs.
  public final static String HMS_RPC_ERROR_FORMAT_STR =
      "Error making '%s' RPC to Hive Metastore: ";
  // The maximum number of partitions to update in one Hive Metastore RPC.
  // Used when persisting the results of COMPUTE STATS statements.
  // It is also used as an upper limit for the number of partitions allowed in one ADD
  // PARTITION statement.
  public final static short MAX_PARTITION_UPDATES_PER_RPC = 500;
  // Error string for inconsistent blacklisted dbs/tables configs between catalogd and
  // coordinators.
  public final static String BLACKLISTED_DBS_INCONSISTENT_ERR_STR =
      "--blacklisted_dbs may be inconsistent between catalogd and coordinators";

  public final static String BLACKLISTED_TABLES_INCONSISTENT_ERR_STR =
      "--blacklisted_tables may be inconsistent between catalogd and coordinators";

  // Table capabilities property name
  private static final String CAPABILITIES_KEY = "OBJCAPABILITIES";

  // Table default capabilities
  private static final String ACIDINSERTONLY_CAPABILITIES =
      "HIVEMANAGEDINSERTREAD,HIVEMANAGEDINSERTWRITE";
  private static final String FULLACID_CAPABILITIES =
      "HIVEFULLACIDREAD";
  private static final String NONACID_CAPABILITIES = "EXTREAD,EXTWRITE";

  // Lock used to ensure that CREATE[DROP] TABLE[DATABASE] operations performed in
  // catalog_ and the corresponding RPC to apply the change in HMS are atomic.
  private static final Object metastoreDdlLock_ = new Object();

  protected final TDdlExecRequest request;
  protected final TDdlExecResponse response;
  protected final CatalogOpExecutor catalogOpExecutor_;
  protected final CatalogServiceCatalog catalog_;
  protected final boolean wantMinimalResult;
  protected final boolean syncDdl;


  public CatalogOperation(TDdlExecRequest ddlExecRequest, TDdlExecResponse response,
      CatalogOpExecutor catalogOpExecutor, boolean wantMinimalResult) {
    this.request = Preconditions.checkNotNull(ddlExecRequest);
    this.response = Preconditions.checkNotNull(response);
    this.catalogOpExecutor_ = Preconditions.checkNotNull(catalogOpExecutor);
    this.catalog_ = catalogOpExecutor_.getCatalog();
    this.wantMinimalResult = wantMinimalResult;
    this.syncDdl = request.isSync_ddl();
  }

  protected abstract boolean takeDdlLock();

  protected abstract void doHmsOperations() throws ImpalaException;

  protected abstract void doCatalogOperations() throws ImpalaException;

  protected abstract void before() throws ImpalaException;

  protected abstract void after() throws ImpalaException;

  public void execute() throws ImpalaException {
    try {
      before();
      if (takeDdlLock()) {
        synchronized (metastoreDdlLock_) {
          doHmsOperations();
          doCatalogOperations();
        }
      } else {
        doHmsOperations();
        doCatalogOperations();
      }
    } finally {
      after();
    }
  }

  /**
   * Drops all table and partition stats from this table in the HMS.
   * Partitions are updated in batches of MAX_PARTITION_UPDATES_PER_RPC. Returns
   * the number of partitions updated as part of this operation, or 1 if the table
   * is unpartitioned.
   */
  protected int dropTableStats(Table table) throws ImpalaException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    // Delete the ROW_COUNT from the table (if it was set).
    org.apache.hadoop.hive.metastore.api.Table msTbl = table.getMetaStoreTable();
    int numTargetedPartitions = 0;
    boolean droppedRowCount =
        msTbl.getParameters().remove(StatsSetupConst.ROW_COUNT) != null;
    boolean droppedTotalSize =
        msTbl.getParameters().remove(StatsSetupConst.TOTAL_SIZE) != null;

    if (droppedRowCount || droppedTotalSize) {
      applyAlterTable(msTbl, false, null);
      ++numTargetedPartitions;
    }

    if (!(table instanceof HdfsTable) || table.getNumClusteringCols() == 0) {
      // If this is not an HdfsTable or if the table is not partitioned, there
      // is no more work to be done so just return.
      return numTargetedPartitions;
    }

    // Now clear the stats for all partitions in the table.
    HdfsTable hdfsTable = (HdfsTable) table;
    Preconditions.checkNotNull(hdfsTable);

    // List of partitions that were modified as part of this operation.
    List<Builder> modifiedParts = Lists.newArrayList();
    Collection<? extends FeFsPartition> parts =
        FeCatalogUtils.loadAllPartitions(hdfsTable);
    for (FeFsPartition fePart: parts) {
      // TODO(todd): avoid downcast
      HdfsPartition part = (HdfsPartition) fePart;
      HdfsPartition.Builder partBuilder = null;
      if (part.getPartitionStatsCompressed() != null) {
        partBuilder = new HdfsPartition.Builder(part).dropPartitionStats();
      }

      // We need to update the partition if it has a ROW_COUNT parameter.
      if (part.getParameters().containsKey(StatsSetupConst.ROW_COUNT)) {
        if (partBuilder == null) {
          partBuilder = new HdfsPartition.Builder(part);
        }
        partBuilder.removeRowCountParam();
      }

      if (partBuilder != null) modifiedParts.add(partBuilder);
    }

    bulkAlterPartitions(table, modifiedParts, null, UpdatePartitionMethod.IN_PLACE);
    return modifiedParts.size();
  }

  /**
   * Drops all column stats from the table in the HMS. Returns the number of columns
   * that were updated as part of this operation.
   */
  protected int dropColumnStats(Table table) throws ImpalaRuntimeException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    int numColsUpdated = 0;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      for (Column col: table.getColumns()) {
        // Skip columns that don't have stats.
        if (!col.getStats().hasStats()) continue;

        try {
          MetastoreShim.deleteTableColumnStatistics(msClient.getHiveClient(),
              table.getDb().getName(), table.getName(), col.getName());
          ++numColsUpdated;
        } catch (NoSuchObjectException e) {
          // We don't care if the column stats do not exist, just ignore the exception.
          // We would only expect to make it here if the Impala and HMS metadata
          // diverged.
        } catch (TException e) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR,
                  "delete_table_column_statistics"), e);
        }
      }
    }
    return numColsUpdated;
  }

  /**
   * Update table properties to remove the COLUMN_STATS_ACCURATE entry if it exists.
   */
  protected void unsetTableColStats(org.apache.hadoop.hive.metastore.api.Table msTable,
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
  protected void unsetPartitionsColStats(org.apache.hadoop.hive.metastore.api.Table msTable,
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

  protected void applyAlterPartition(Table tbl, HdfsPartition.Builder partBuilder)
      throws ImpalaException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      Partition hmsPartition = partBuilder.toHmsPartition();
      addCatalogServiceIdentifiers(catalog_, tbl.getMetaStoreTable(), hmsPartition);
      applyAlterHmsPartitions(tbl.getMetaStoreTable().deepCopy(), msClient,
          tbl.getTableName(), Arrays.asList(hmsPartition));
      addToInflightVersionsOfPartition(hmsPartition.getParameters(), partBuilder);
    }
  }

  protected void applyAlterHmsPartitions(org.apache.hadoop.hive.metastore.api.Table msTbl,
      MetaStoreClient msClient, TableName tableName, List<Partition> hmsPartitions)
      throws ImpalaException {
    try {
      MetastoreShim.alterPartitions(
          msClient.getHiveClient(), tableName.getDb(), tableName.getTbl(), hmsPartitions);
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_partitions"), e);
    }
  }

  protected static void setDefaultTableCapabilities(
      org.apache.hadoop.hive.metastore.api.Table tbl) {
    if (MetastoreShim.getMajorVersion() > 2) {
      // This hms table is for create table.
      // It needs read/write access type,  not default value(0, undefined)
      MetastoreShim.setTableAccessType(tbl, ACCESSTYPE_READWRITE);
      // Set table default capabilities in HMS
      if (tbl.getParameters().containsKey(CAPABILITIES_KEY)) return;
      if (AcidUtils.isTransactionalTable(tbl.getParameters())) {
        if (AcidUtils.isFullAcidTable(tbl.getParameters())) {
          tbl.getParameters().put(CAPABILITIES_KEY, FULLACID_CAPABILITIES);
        } else {
          tbl.getParameters().put(CAPABILITIES_KEY, ACIDINSERTONLY_CAPABILITIES);
        }
      } else {
        // Managed KUDU table has issues with extra table properties:
        // 1. The property is not stored. 2. The table cannot be found after created.
        // Related jira: IMPALA-8751
        // Skip adding default capabilities for KUDU tables before the issues are fixed.
        if (!KuduTable.isKuduTable(tbl)) {
          tbl.getParameters().put(CAPABILITIES_KEY, NONACID_CAPABILITIES);
        }
      }
    }
  }



  /**
   * Utility function that creates a hive.metastore.api.Table object based on the given
   * TCreateTableParams.
   * TODO: Extract metastore object creation utility functions into a separate
   * helper/factory class.
   */
  public static org.apache.hadoop.hive.metastore.api.Table createMetaStoreTable(
      TCreateTableParams params) {
    Preconditions.checkNotNull(params);
    TableName tableName = TableName.fromThrift(params.getTable_name());
    org.apache.hadoop.hive.metastore.api.Table tbl =
        new org.apache.hadoop.hive.metastore.api.Table();
    tbl.setDbName(tableName.getDb());
    tbl.setTableName(tableName.getTbl());
    tbl.setOwner(params.getOwner());

    if (params.isSetTable_properties()) {
      tbl.setParameters(params.getTable_properties());
    } else {
      tbl.setParameters(new HashMap<String, String>());
    }

    if (params.isSetSort_columns() && !params.sort_columns.isEmpty()) {
      tbl.getParameters().put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS,
          Joiner.on(",").join(params.sort_columns));
      TSortingOrder sortingOrder = params.isSetSorting_order() ?
          params.sorting_order : TSortingOrder.LEXICAL;
      tbl.getParameters().put(AlterTableSortByStmt.TBL_PROP_SORT_ORDER,
          sortingOrder.toString());
    }
    if (params.getComment() != null) {
      tbl.getParameters().put("comment", params.getComment());
    }
    if (params.is_external) {
      tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      tbl.putToParameters("EXTERNAL", "TRUE");
    } else {
      tbl.setTableType(TableType.MANAGED_TABLE.toString());
    }

    tbl.setSd(createSd(params));
    if (params.getPartition_columns() != null) {
      // Add in any partition keys that were specified
      tbl.setPartitionKeys(buildFieldSchemaList(params.getPartition_columns()));
    } else {
      tbl.setPartitionKeys(new ArrayList<FieldSchema>());
    }

    setDefaultTableCapabilities(tbl);
    return tbl;
  }

  private static StorageDescriptor createSd(TCreateTableParams params) {
    StorageDescriptor sd = HiveStorageDescriptorFactory.createSd(
        params.getFile_format(), RowFormat.fromThrift(params.getRow_format()));
    if (params.isSetSerde_properties()) {
      if (sd.getSerdeInfo().getParameters() == null) {
        sd.getSerdeInfo().setParameters(params.getSerde_properties());
      } else {
        sd.getSerdeInfo().getParameters().putAll(params.getSerde_properties());
      }
    }

    if (params.getLocation() != null) sd.setLocation(params.getLocation());

    // Add in all the columns
    sd.setCols(buildFieldSchemaList(params.getColumns()));
    return sd;
  }

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

  protected static boolean isHmsIntegrationAutomatic(
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws ImpalaRuntimeException {
    if (KuduTable.isKuduTable(msTbl)) {
      return isKuduHmsIntegrationEnabled(msTbl);
    }
    if (IcebergTable.isIcebergTable(msTbl)) {
      return isIcebergHmsIntegrationEnabled(msTbl);
    }
    return false;
  }


  /**
   * Drops all associated caching requests on the table and/or table's partitions,
   * uncaching all table data, if applicable. Throws no exceptions, only logs errors.
   * Does not update the HMS.
   */
  protected static void uncacheTable(FeTable table) {
    if (!(table instanceof FeFsTable)) return;
    FeFsTable hdfsTable = (FeFsTable) table;
    if (hdfsTable.isMarkedCached()) {
      try {
        HdfsCachingUtil.removeTblCacheDirective(table.getMetaStoreTable());
      } catch (Exception e) {
        LOG.error("Unable to uncache table: " + table.getFullName(), e);
      }
    }
    if (table.getNumClusteringCols() > 0) {
      Collection<? extends FeFsPartition> parts =
          FeCatalogUtils.loadAllPartitions(hdfsTable);
      for (FeFsPartition part: parts) {
        if (part.isMarkedCached()) {
          HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(
              (HdfsPartition) part);
          try {
            HdfsCachingUtil.removePartitionCacheDirective(partBuilder);
            // We are dropping the table. Don't need to update the existing partition so
            // ignore the partBuilder here.
          } catch (Exception e) {
            LOG.error("Unable to uncache partition: " + part.getPartitionName(), e);
          }
        }
      }
    }
  }

  protected static boolean isIcebergHmsIntegrationEnabled(
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws ImpalaRuntimeException {
    // Check if Iceberg's integration with the Hive Metastore is enabled, and validate
    // the configuration.
    Preconditions.checkState(IcebergTable.isIcebergTable(msTbl));
    // Only synchronized tables can be integrated.
    if (!IcebergTable.isSynchronizedTable(msTbl)) {
      return false;
    }
    return IcebergUtil.getTIcebergCatalog(msTbl) == TIcebergCatalog.HIVE_CATALOG;
  }

  protected static boolean isKuduHmsIntegrationEnabled(
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws ImpalaRuntimeException {
    // Check if Kudu's integration with the Hive Metastore is enabled, and validate
    // the configuration.
    Preconditions.checkState(KuduTable.isKuduTable(msTbl));
    String masterHosts = msTbl.getParameters().get(KuduTable.KEY_MASTER_HOSTS);
    String hmsUris = MetaStoreUtil.getHiveMetastoreUris();
    return KuduTable.isHMSIntegrationEnabledAndValidate(masterHosts, hmsUris);
  }

  protected static enum UpdatePartitionMethod {
    // Do not apply updates to the partition. The caller is responsible for updating
    // the state of any modified partitions to reflect changes applied.
    NONE,
    // Update the state of the Partition objects in place in the catalog.
    IN_PLACE,
    // Mark the partition dirty so that it will be later reloaded from scratch when
    // the table is reloaded.
    MARK_DIRTY,
  }

  /**
   * Helper method for setting the file format on a given storage descriptor.
   */
  public static void setStorageDescriptorFileFormat(StorageDescriptor sd,
      THdfsFileFormat fileFormat) {
    StorageDescriptor tempSd =
        HiveStorageDescriptorFactory.createSd(fileFormat, RowFormat.DEFAULT_ROW_FORMAT);
    sd.setInputFormat(tempSd.getInputFormat());
    sd.setOutputFormat(tempSd.getOutputFormat());
    sd.getSerdeInfo().setSerializationLib(tempSd.getSerdeInfo().getSerializationLib());
  }

  /**
   * Serializes and adds table 'tbl' to a TCatalogUpdateResult object. Uses the version of
   * the serialized table as the version of the catalog update result.
   */
  public static void addTableToCatalogUpdate(Table tbl, boolean wantMinimalResult,
      TCatalogUpdateResult result) {
    Preconditions.checkNotNull(tbl);
    // TODO(IMPALA-9937): if client is a 'v1' impalad, only send back incremental updates
    TCatalogObject updatedCatalogObject = wantMinimalResult ?
        tbl.toInvalidationObject() : tbl.toTCatalogObject();
    result.addToUpdated_catalog_objects(updatedCatalogObject);
    result.setVersion(updatedCatalogObject.getCatalog_version());
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
    try (MetaStoreClient msClient = catalogOpExecutor_.getCatalog()
        .getMetaStoreClient()) {
      applyAlterTable(msClient, msTbl, overwriteLastDdlTime, tblTxn);
    }
  }

  public static void applyAlterTable(MetaStoreClient msClient,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      boolean overwriteLastDdlTime, TblTransaction tblTxn) throws ImpalaRuntimeException {
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

  /**
   * Adds the catalog service id and the given catalog version to the table
   * parameters. No-op if event processing is disabled
   */
  public static void addCatalogServiceIdentifiers(
      CatalogServiceCatalog catalog, Table tbl, long newCatalogVersion) {
    if (!catalog.isEventProcessingActive()) return;
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable();
    msTbl.putToParameters(
        MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(),
        catalog.getCatalogServiceId());
    msTbl.putToParameters(
        MetastoreEventPropertyKey.CATALOG_VERSION.getKey(),
        String.valueOf(newCatalogVersion));
  }

  /**
   * Adds the catalog service id and the given catalog version to the database parameters.
   * No-op if event processing is disabled
   */
  public static void addCatalogServiceIdentifiers(
      CatalogServiceCatalog catalog, Database msDb, long newCatalogVersion) {
    if (!catalog.isEventProcessingActive()) {
      return;
    }
    Preconditions.checkNotNull(msDb);
    msDb.putToParameters(MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(),
        catalog.getCatalogServiceId());
    msDb.putToParameters(MetastoreEventPropertyKey.CATALOG_VERSION.getKey(),
        String.valueOf(newCatalogVersion));
  }

  /**
   * No-op if event processing is disabled. Adds this catalog service id and the given
   * catalog version to the partition parameters from table parameters.
   */
  public static void addCatalogServiceIdentifiers(
      CatalogServiceCatalog catalog, org.apache.hadoop.hive.metastore.api.Table msTbl,
      Partition partition) {
    if (!catalog.isEventProcessingActive()) {
      return;
    }
    Preconditions.checkState(msTbl.isSetParameters());
    Preconditions.checkNotNull(partition, "Partition is null");
    Map<String, String> tblParams = msTbl.getParameters();
    Preconditions
        .checkState(tblParams.containsKey(
            MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey()),
            "Table parameters must have catalog service identifier before "
                + "adding it to partition parameters");
    Preconditions
        .checkState(tblParams.containsKey(
            MetastoreEventPropertyKey.CATALOG_VERSION.getKey()),
            "Table parameters must contain catalog version before adding "
                + "it to partition parameters");
    // make sure that the service id from the table matches with our own service id to
    // avoid issues where the msTbl has an older (other catalogs' service identifiers)
    String serviceIdFromTbl =
        tblParams.get(MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey());
    String version = tblParams.get(MetastoreEventPropertyKey.CATALOG_VERSION.getKey());
    if (catalog.getCatalogServiceId().equals(serviceIdFromTbl)) {
      partition.putToParameters(
          MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(), serviceIdFromTbl);
      partition.putToParameters(
          MetastoreEventPropertyKey.CATALOG_VERSION.getKey(), version);
    }
  }


  /**
   * Alters partitions in the HMS in batches of size 'MAX_PARTITION_UPDATES_PER_RPC'. This
   * reduces the time spent in a single update and helps avoid metastore client timeouts.
   *
   * @param updateMethod controls how the same updates are applied to 'tbl' to reflect the
   *                     changes written to the HMS.
   */
  protected void bulkAlterPartitions(Table tbl, List<Builder> modifiedParts,
      TblTransaction tblTxn, UpdatePartitionMethod updateMethod) throws ImpalaException {
    // Map from msPartitions to the partition builders. Use IdentityHashMap since
    // modifications will change hash codes of msPartitions.
    Map<Partition, HdfsPartition.Builder> msPartitionToBuilders =
        Maps.newIdentityHashMap();
    for (HdfsPartition.Builder p : modifiedParts) {
      Partition msPart = p.toHmsPartition();
      if (msPart != null) {
        addCatalogServiceIdentifiers(catalog_, tbl.getMetaStoreTable(), msPart);
        msPartitionToBuilders.put(msPart, p);
      }
    }
    if (msPartitionToBuilders.isEmpty()) {
      return;
    }

    String dbName = tbl.getDb().getName();
    String tableName = tbl.getName();
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      // Apply the updates in batches of 'MAX_PARTITION_UPDATES_PER_RPC'.
      for (List<Partition> msPartitionsSubList : Iterables.partition(
          msPartitionToBuilders.keySet(), MAX_PARTITION_UPDATES_PER_RPC)) {
        try {
          // Alter partitions in bulk.
          if (tblTxn != null) {
            MetastoreShim.alterPartitionsWithTransaction(msClient.getHiveClient(), dbName,
                tableName, msPartitionsSubList, tblTxn);
          } else {
            MetastoreShim.alterPartitions(msClient.getHiveClient(), dbName, tableName,
                msPartitionsSubList);
          }
          // Mark the corresponding HdfsPartition objects as dirty
          for (Partition msPartition : msPartitionsSubList) {
            HdfsPartition.Builder partBuilder = msPartitionToBuilders.get(msPartition);
            Preconditions.checkNotNull(partBuilder);
            // The partition either needs to be reloaded or updated in place to apply
            // the modifications.
            if (updateMethod == UpdatePartitionMethod.MARK_DIRTY) {
              ((HdfsTable) tbl).markDirtyPartition(partBuilder);
            } else if (updateMethod == UpdatePartitionMethod.IN_PLACE) {
              ((HdfsTable) tbl).updatePartition(partBuilder);
            }
            // If event processing is turned on add the version number from partition
            // parameters to the HdfsPartition's list of in-flight events.
            addToInflightVersionsOfPartition(msPartition.getParameters(), partBuilder);
          }
        } catch (TException e) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_partitions"), e);
        }
      }
    }
  }

  protected static List<FieldSchema> buildFieldSchemaList(List<TColumn> columns) {
    List<FieldSchema> fsList = Lists.newArrayList();
    // Add in all the columns
    for (TColumn col : columns) {
      Type type = Type.fromThrift(col.getColumnType());
      // The type string must be lowercase for Hive to read the column metadata properly.
      String typeSql = type.toSql().toLowerCase();
      FieldSchema fs = new FieldSchema(col.getColumnName(), typeSql, col.getComment());
      fsList.add(fs);
    }
    return fsList;
  }

  /**
   * Updates the database object in the metastore.
   */
  protected void applyAlterDatabase(Database msDb)
      throws ImpalaRuntimeException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().alterDatabase(msDb.getName(), msDb);
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "alterDatabase"), e);
    }
  }

  protected void addDbToCatalogUpdate(Db db, boolean wantMinimalResult,
      TCatalogUpdateResult result) {
    Preconditions.checkNotNull(db);
    TCatalogObject updatedCatalogObject = wantMinimalResult ?
        db.toMinimalTCatalogObject() : db.toTCatalogObject();
    updatedCatalogObject.setCatalog_version(updatedCatalogObject.getCatalog_version());
    result.addToUpdated_catalog_objects(updatedCatalogObject);
    result.setVersion(updatedCatalogObject.getCatalog_version());
  }

  /**
   * This method extracts the catalog version from the tbl parameters and adds it to the
   * HdfsPartition's inflight events. This information is used by event processor to skip
   * the event generated on the partition.
   */
  protected void addToInflightVersionsOfPartition(
      Map<String, String> partitionParams, HdfsPartition.Builder partBuilder) {
    if (!catalog_.isEventProcessingActive()) {
      return;
    }
    Preconditions.checkState(partitionParams != null);
    String version = partitionParams
        .get(MetastoreEventPropertyKey.CATALOG_VERSION.getKey());
    String serviceId = partitionParams
        .get(MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey());

    // make sure that we are adding the catalog version from our own instance of
    // catalog service identifiers
    if (catalog_.getCatalogServiceId().equals(serviceId)) {
      Preconditions.checkNotNull(version);
      partBuilder.addToVersionsForInflightEvents(false, Long.parseLong(version));
    }
  }
}
