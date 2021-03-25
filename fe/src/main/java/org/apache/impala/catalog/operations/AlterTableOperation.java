package org.apache.impala.catalog.operations;

import com.codahale.metrics.Timer.Context;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.impala.analysis.AlterTableSortByStmt;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnNotFoundException;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.HiveStorageDescriptorFactory;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.PartitionNotFoundException;
import org.apache.impala.catalog.PartitionStatsUtil;
import org.apache.impala.catalog.RowFormat;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.IcebergCatalogOpExecutor;
import org.apache.impala.service.KuduCatalogOpExecutor;
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
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.THdfsCachingOp;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TPartitionDef;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.thrift.TRangePartitionOperationType;
import org.apache.impala.thrift.TTableRowFormat;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.AcidUtils.TblTransaction;
import org.apache.impala.util.DebugUtils;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlterTableOperation extends CatalogDdlOperation {

  private static final Logger LOG = LoggerFactory.getLogger(AlterTableOperation.class);

  // the table on which alter operation needs to be executed
  private Table tbl;
  // the new catalog version which needs to be assigned to the table
  // once the alter operation is completed.
  private long newCatalogVersion;
  // When true, loads the table schema and the column stats from the Hive Metastore.
  private boolean reloadTableSchema;
  // in case of add/drop partitions we use this flag to skip the table reload
  // since the partitions are added/dropped by alter operation are explicitly reloaded.
  private boolean reloadMetadata = true;
  // if the alter operations adds new Partitions they are collected in this list so
  // that catalog operation part can add them to the Catalog Table.
  private final List<Partition> addedHmsPartitions = Lists.newArrayList();
  private final List<HdfsPartition> droppedPartitions = Lists.newArrayList();
  // When true, loads the file/block metadata.
  private boolean reloadFileMetadata;
  // keeps track of number of updated partitions, mostly used to give a summary
  // message in the response.
  private final Reference<Long> numUpdatedPartitions = new Reference<>(0L);
  // List of cache directive IDs that were submitted as part of this
  // ALTER TABLE operation.
  private final List<Long> cacheDirIds = Lists.newArrayList();
  private String cacheDirectiveReason = null;

  // keep track of old ownerType and owner in case alter operation changes
  // owner information
  private PrincipalType oldOwnerType;
  private String oldOwner;
  private Context context;

  public AlterTableOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response, CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
  }

  @Override
  protected boolean requiresDdlLock() {
    return false;
  }

  /**
   * Executes the ALTER TABLE command for a Kudu table and reloads its metadata.
   */
  private void alterKuduTable(TAlterTableParams params, TDdlExecResponse response,
      KuduTable tbl) throws ImpalaRuntimeException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    switch (params.getAlter_type()) {
      case ADD_COLUMNS:
        TAlterTableAddColsParams addColParams = params.getAdd_cols_params();
        KuduCatalogOpExecutor.addColumn(tbl, addColParams.getColumns());
        addSummary(response, "Column(s) have been added.");
        break;
      case REPLACE_COLUMNS:
        TAlterTableReplaceColsParams replaceColParams = params.getReplace_cols_params();
        KuduCatalogOpExecutor.addColumn(tbl, replaceColParams.getColumns());
        addSummary(response, "Column(s) have been replaced.");
        break;
      case DROP_COLUMN:
        TAlterTableDropColParams dropColParams = params.getDrop_col_params();
        KuduCatalogOpExecutor.dropColumn(tbl, dropColParams.getCol_name());
        addSummary(response, "Column has been dropped.");
        break;
      case ALTER_COLUMN:
        TAlterTableAlterColParams alterColParams = params.getAlter_col_params();
        KuduCatalogOpExecutor.alterColumn(tbl, alterColParams.getCol_name(),
            alterColParams.getNew_col_def());
        addSummary(response, "Column has been altered.");
        break;
      case ADD_DROP_RANGE_PARTITION:
        TAlterTableAddDropRangePartitionParams partParams =
            params.getAdd_drop_range_partition_params();
        KuduCatalogOpExecutor.addDropRangePartition(tbl, partParams);
        addSummary(response, "Range partition has been " +
            (partParams.type == TRangePartitionOperationType.ADD ?
                "added." : "dropped."));
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported ALTER TABLE operation for Kudu tables: " +
                params.getAlter_type());
    }
  }

  /**
   * Returns true if the given alteration type changes the underlying table stored in
   * Iceberg in addition to the HMS table.
   */
  private boolean altersIcebergTable(TAlterTableType type) {
    return type == TAlterTableType.ADD_COLUMNS
        || type == TAlterTableType.REPLACE_COLUMNS
        || type == TAlterTableType.DROP_COLUMN
        || type == TAlterTableType.ALTER_COLUMN;
  }


  /**
   * Executes the ALTER TABLE command for a Iceberg table and reloads its metadata.
   */
  private void alterIcebergTable(TAlterTableParams params, TDdlExecResponse response,
      IcebergTable tbl) throws ImpalaRuntimeException, TableLoadingException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    switch (params.getAlter_type()) {
      case ADD_COLUMNS:
        TAlterTableAddColsParams addColParams = params.getAdd_cols_params();
        IcebergCatalogOpExecutor.addColumn(tbl, addColParams.getColumns());
        addSummary(response, "Column(s) have been added.");
        break;
      case REPLACE_COLUMNS:
        //TODO: we need support resolve column by field id at first, and then
        // support this statement
      case DROP_COLUMN:
        //TODO: we need support resolve column by field id at first, and then
        // support this statement
        //TAlterTableDropColParams dropColParams = params.getDrop_col_params();
        //IcebergCatalogOpExecutor.dropColumn(tbl, dropColParams.getCol_name());
        //addSummary(response, "Column has been dropped.");
      case ALTER_COLUMN:
        //TODO: we need support resolve column by field id at first, and then
        // support this statement
        //TAlterTableAlterColParams alterColParams = params.getAlter_col_params();
        //IcebergCatalogOpExecutor.alterColumn(tbl, alterColParams.getCol_name(),
        //    alterColParams.getNew_col_def());
        //addSummary(response, "Column has been altered.");
      default:
        throw new UnsupportedOperationException(
            "Unsupported ALTER TABLE operation for Iceberg tables: " +
                params.getAlter_type());
    }
  }

  /**
   * Renames an existing table or view. After renaming the table/view, its metadata is
   * marked as invalid and will be reloaded on the next access.
   */
  private void alterTableOrViewRename(Table oldTbl, TableName newTableName,
      TDdlExecResponse response) throws ImpalaException {
    Preconditions.checkState(oldTbl.isWriteLockedByCurrentThread()
        && catalog_.getLock().isWriteLockedByCurrentThread());
    TableName tableName = oldTbl.getTableName();
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        oldTbl.getMetaStoreTable().deepCopy();
    msTbl.setDbName(newTableName.getDb());
    msTbl.setTableName(newTableName.getTbl());

    // If oldTbl is a synchronized Kudu table, rename the underlying Kudu table.
    boolean isSynchronizedKuduTable = (oldTbl instanceof KuduTable) &&
        KuduTable.isSynchronizedTable(msTbl);
    boolean integratedHmsTable = isHmsIntegrationAutomatic(msTbl);
    if (isSynchronizedKuduTable) {
      Preconditions.checkState(KuduTable.isKuduTable(msTbl));
      renameManagedKuduTable((KuduTable) oldTbl, msTbl, newTableName, integratedHmsTable);
    }

    // If oldTbl is a synchronized Iceberg table, rename the underlying Iceberg table.
    boolean isSynchronizedIcebergTable = (oldTbl instanceof IcebergTable) &&
        IcebergTable.isSynchronizedTable(msTbl);
    if (isSynchronizedIcebergTable) {
      renameManagedIcebergTable((IcebergTable) oldTbl, msTbl, newTableName);
    }

    boolean isSynchronizedTable = isSynchronizedKuduTable || isSynchronizedIcebergTable;
    // Update the HMS table, unless the table is synchronized and the HMS integration
    // is automatic.
    boolean needsHmsAlterTable = !isSynchronizedTable || !integratedHmsTable;
    if (needsHmsAlterTable) {
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        msClient.getHiveClient().alter_table(
            tableName.getDb(), tableName.getTbl(), msTbl);
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_table"), e);
      }
    }
    addSummary(response, "Renaming was successful.");
  }


  /**
   * Renames the underlying Kudu table for the given managed table. If the new Kudu table
   * name is the same as the old Kudu table name, this method does nothing.
   */
  private void renameManagedKuduTable(KuduTable oldTbl,
      org.apache.hadoop.hive.metastore.api.Table oldMsTbl,
      TableName newTableName, boolean isHMSIntegrationEanbled)
      throws ImpalaRuntimeException {
    String newKuduTableName = KuduUtil.getDefaultKuduTableName(
        newTableName.getDb(), newTableName.getTbl(),
        isHMSIntegrationEanbled);

    // If the name of the Kudu table has not changed, do nothing
    if (oldTbl.getKuduTableName().equals(newKuduTableName)) {
      return;
    }

    KuduCatalogOpExecutor.renameTable(oldTbl, newKuduTableName);

    // Add the name of the new Kudu table to the HMS table parameters
    oldMsTbl.getParameters().put(KuduTable.KEY_TABLE_NAME, newKuduTableName);
  }

  /**
   * Renames the underlying Iceberg table for the given managed table. If the new Iceberg
   * table name is the same as the old Iceberg table name, this method does nothing.
   */
  private void renameManagedIcebergTable(IcebergTable oldTbl,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      TableName newTableName) throws ImpalaRuntimeException {
    TableIdentifier tableId = TableIdentifier.of(newTableName.getDb(),
        newTableName.getTbl());
    IcebergCatalogOpExecutor.renameTable(oldTbl, tableId);

    if (msTbl.getParameters().get(IcebergTable.ICEBERG_TABLE_IDENTIFIER) != null) {
      // We need update table identifier for HadoopCatalog managed table if exists.
      msTbl.getParameters().put(IcebergTable.ICEBERG_TABLE_IDENTIFIER,
          tableId.toString());
    }
  }

  /**
   * Appends one or more columns to the given table. Returns true if there a column was
   * added; false otherwise.
   */
  private boolean alterTableAddCols(Table tbl, List<TColumn> columns, boolean ifNotExists)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    List<TColumn> colsToAdd = new ArrayList<>();
    for (TColumn column : columns) {
      Column col = tbl.getColumn(column.getColumnName());
      if (ifNotExists && col != null) {
        continue;
      }
      if (col != null) {
        throw new CatalogException(
            String.format("Column '%s' in table '%s' already exists.",
                col.getName(), tbl.getName()));
      }
      colsToAdd.add(column);
    }
    // Only add columns that do not exist.
    if (!colsToAdd.isEmpty()) {
      // Append the new column to the existing list of columns.
      msTbl.getSd().getCols().addAll(buildFieldSchemaList(colsToAdd));
      applyAlterTable(msTbl);
      return true;
    }
    return false;
  }


  /**
   * Replaces all existing columns to the given table.
   */
  private void alterTableReplaceCols(Table tbl, List<TColumn> columns)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    List<FieldSchema> newColumns = buildFieldSchemaList(columns);
    msTbl.getSd().setCols(newColumns);
    String sortByKey = AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS;
    if (msTbl.getParameters().containsKey(sortByKey)) {
      String oldColumns = msTbl.getParameters().get(sortByKey);
      String alteredColumns = MetaStoreUtil.intersectCsvListWithColumNames(oldColumns,
          columns);
      msTbl.getParameters().put(sortByKey, alteredColumns);
    }
    applyAlterTable(msTbl);
  }

  /**
   * Create a new HMS Partition.
   */
  private Partition createHmsPartition(List<TPartitionKeyValue> partitionSpec,
      org.apache.hadoop.hive.metastore.api.Table msTbl, TableName tableName,
      String location) {
    List<String> values = Lists.newArrayList();
    // Need to add in the values in the same order they are defined in the table.
    for (FieldSchema fs : msTbl.getPartitionKeys()) {
      for (TPartitionKeyValue kv : partitionSpec) {
        if (fs.getName().toLowerCase().equals(kv.getName().toLowerCase())) {
          values.add(kv.getValue());
        }
      }
    }
    return createHmsPartitionFromValues(values, msTbl, tableName, location);
  }


  /**
   * Create a new HMS Partition from partition values.
   */
  private Partition createHmsPartitionFromValues(List<String> partitionSpecValues,
      org.apache.hadoop.hive.metastore.api.Table msTbl, TableName tableName,
      String location) {
    // Create HMS Partition.
    org.apache.hadoop.hive.metastore.api.Partition partition =
        new org.apache.hadoop.hive.metastore.api.Partition();
    partition.setDbName(tableName.getDb());
    partition.setTableName(tableName.getTbl());
    partition.setValues(partitionSpecValues);
    StorageDescriptor sd = msTbl.getSd().deepCopy();
    sd.setLocation(location);
    partition.setSd(sd);
    // if external event processing is enabled, add the catalog service identifiers
    // from table to the partition
    addCatalogServiceIdentifiers(catalog_, msTbl, partition);
    return partition;
  }

  /**
   * Adds new partitions to the given table in HMS. Also creates and adds new
   * HdfsPartitions to the corresponding HdfsTable. Returns the table object with an
   * updated catalog version or null if the table is not altered because all the
   * partitions already exist and IF NOT EXISTS is specified. If IF NOT EXISTS is not used
   * and there is a conflict with the partitions that already exist in HMS or catalog
   * cache, then: - HMS and catalog cache are left intact, and - ImpalaRuntimeException is
   * thrown. If IF NOT EXISTS is used, conflicts are handled as follows: 1. If a partition
   * exists in catalog cache, ignore it. 2. If a partition exists in HMS but not in
   * catalog cache, reload partition from HMS. Caching directives are only applied to new
   * partitions that were absent from both the catalog cache and the HMS.
   */
  private void alterTableAddPartitions(Table tbl,
      TAlterTableAddPartitionParams addPartParams) throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());

    TableName tableName = tbl.getTableName();
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    boolean ifNotExists = addPartParams.isIf_not_exists();
    List<Partition> allHmsPartitionsToAdd = Lists.newArrayList();
    Map<List<String>, THdfsCachingOp> partitionCachingOpMap = Maps.newHashMap();
    for (TPartitionDef partParams : addPartParams.getPartitions()) {
      List<TPartitionKeyValue> partitionSpec = partParams.getPartition_spec();
      if (catalog_.containsHdfsPartition(tableName.getDb(), tableName.getTbl(),
          partitionSpec)) {
        String partitionSpecStr = Joiner.on(", ").join(partitionSpec);
        if (!ifNotExists) {
          throw new ImpalaRuntimeException(String.format("Partition already " +
              "exists: (%s)", partitionSpecStr));
        }
        LOG.trace(String.format("Skipping partition creation because (%s) already " +
            "exists and IF NOT EXISTS was specified.", partitionSpecStr));
        continue;
      }

      Partition hmsPartition =
          createHmsPartition(partitionSpec, msTbl, tableName, partParams.getLocation());
      allHmsPartitionsToAdd.add(hmsPartition);

      THdfsCachingOp cacheOp = partParams.getCache_op();
      if (cacheOp != null) {
        partitionCachingOpMap.put(hmsPartition.getValues(), cacheOp);
      }
    }

    if (allHmsPartitionsToAdd.isEmpty()) {
      return;
    }

    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {

      for (List<Partition> hmsSublist :
          Lists.partition(allHmsPartitionsToAdd, MAX_PARTITION_UPDATES_PER_RPC)) {
        try {
          addedHmsPartitions.addAll(msClient.getHiveClient().add_partitions(hmsSublist,
              ifNotExists, true));
        } catch (TException e) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "add_partitions"), e);
        }
      }
      // Handle HDFS cache. This is done in a separate round bacause we have to apply
      // caching only to newly added partitions.
      alterTableCachePartitions(msTbl, msClient, tableName, addedHmsPartitions,
          partitionCachingOpMap);

      // If 'ifNotExists' is true, add_partitions() may fail to add all the partitions to
      // HMS because some of them may already exist there. In that case, we load in the
      // catalog the partitions that already exist in HMS but aren't in the catalog yet.
      if (allHmsPartitionsToAdd.size() != addedHmsPartitions.size()) {
        List<Partition> difference = computeDifference(allHmsPartitionsToAdd,
            addedHmsPartitions);
        addedHmsPartitions.addAll(
            getPartitionsFromHms(msTbl, msClient, tableName, difference));
      }
    }
  }

  /**
   * Returns a list of partitions retrieved from HMS for each 'hmsPartitions' element.
   */
  private List<Partition> getPartitionsFromHms(
      org.apache.hadoop.hive.metastore.api.Table msTbl, MetaStoreClient msClient,
      TableName tableName, List<Partition> hmsPartitions)
      throws ImpalaException {
    List<String> partitionCols = Lists.newArrayList();
    for (FieldSchema fs : msTbl.getPartitionKeys()) {
      partitionCols.add(fs.getName());
    }

    List<String> partitionNames = Lists.newArrayListWithCapacity(hmsPartitions.size());
    for (Partition part : hmsPartitions) {
      String partName = org.apache.hadoop.hive.common.FileUtils.makePartName(
          partitionCols, part.getValues());
      partitionNames.add(partName);
    }
    try {
      return msClient.getHiveClient().getPartitionsByNames(tableName.getDb(),
          tableName.getTbl(), partitionNames);
    } catch (TException e) {
      throw new ImpalaRuntimeException("Metadata inconsistency has occured. Please run "
          + "'invalidate metadata <tablename>' to resolve the problem.", e);
    }
  }

  /**
   * Returns the list of Partition objects from 'aList' that cannot be found in 'bList'.
   * Partition objects are distinguished by partition values only.
   */
  private List<Partition> computeDifference(List<Partition> aList,
      List<Partition> bList) {
    Set<List<String>> bSet = Sets.newHashSet();
    for (Partition b : bList) {
      bSet.add(b.getValues());
    }

    List<Partition> diffList = Lists.newArrayList();
    for (Partition a : aList) {
      if (!bSet.contains(a.getValues())) {
        diffList.add(a);
      }
    }
    return diffList;
  }

  /**
   * Applies HDFS caching ops on 'hmsPartitions' and updates their metadata in Hive
   * Metastore. 'partitionCachingOpMap' maps partitions (identified by their partition
   * values) to their corresponding HDFS caching ops.
   */
  private void alterTableCachePartitions(org.apache.hadoop.hive.metastore.api.Table msTbl,
      MetaStoreClient msClient, TableName tableName, List<Partition> hmsPartitions,
      Map<List<String>, THdfsCachingOp> partitionCachingOpMap)
      throws ImpalaException {
    List<Partition> hmsPartitionsToCache = Lists.newArrayList();
    Long parentTblCacheDirId = HdfsCachingUtil.getCacheDirectiveId(msTbl.getParameters());
    for (Partition partition : hmsPartitions) {
      THdfsCachingOp cacheOp = partitionCachingOpMap.get(partition.getValues());
      String cachePoolName = null;
      Short replication = null;
      if (cacheOp == null && parentTblCacheDirId != null) {
        // The user didn't specify an explicit caching operation, inherit the value
        // from the parent table.
        cachePoolName = HdfsCachingUtil.getCachePool(parentTblCacheDirId);
        Preconditions.checkNotNull(cachePoolName);
        replication = HdfsCachingUtil.getCacheReplication(parentTblCacheDirId);
        Preconditions.checkNotNull(replication);
      } else if (cacheOp != null && cacheOp.isSet_cached()) {
        // The user explicitly stated that this partition should be cached.
        cachePoolName = cacheOp.getCache_pool_name();

        // When the new partition should be cached and and no replication factor
        // was specified, inherit the replication factor from the parent table if
        // it is cached. If the parent is not cached and no replication factor is
        // explicitly set, use the default value.
        if (!cacheOp.isSetReplication() && parentTblCacheDirId != null) {
          replication = HdfsCachingUtil.getCacheReplication(parentTblCacheDirId);
        } else {
          replication = HdfsCachingUtil.getReplicationOrDefault(cacheOp);
        }
      }
      // If cache pool name is not null, it indicates this partition should be cached.
      if (cachePoolName != null) {
        long id = HdfsCachingUtil.submitCachePartitionDirective(partition,
            cachePoolName, replication);
        cacheDirIds.add(id);
        hmsPartitionsToCache.add(partition);
      }
    }

    // Update the partition metadata to include the cache directive id.
    if (!cacheDirIds.isEmpty()) {
      applyAlterHmsPartitions(msTbl, msClient, tableName, hmsPartitionsToCache);
      cacheDirectiveReason = "ALTER TABLE CACHE PARTITIONS";
    }
  }

  @Override
  public void doHmsOperations() throws ImpalaException {
    if (isRenameOperation()) {
      TableName newTblName = TableName.fromThrift(
          request.getAlter_table_params().getRename_params().getNew_table_name());
      alterTableOrViewRename(tbl, newTblName, response);
      return;
    }
    TAlterTableParams params = request.alter_table_params;
    if (tbl instanceof KuduTable && altersKuduTable(params.getAlter_type())) {
      alterKuduTable(params, response, (KuduTable) tbl);
      return;
    } else if (tbl instanceof IcebergTable &&
        altersIcebergTable(params.getAlter_type())) {
      alterIcebergTable(params, response, (IcebergTable) tbl);
      return;
    }
    switch (params.getAlter_type()) {
      case ADD_COLUMNS:
        TAlterTableAddColsParams addColParams = params.getAdd_cols_params();
        boolean added = alterTableAddCols(tbl, addColParams.getColumns(),
            addColParams.isIf_not_exists());
        reloadTableSchema = true;
        if (added) {
          addSummary(response, "New column(s) have been added to the table.");
        } else {
          addSummary(response, "No new column(s) have been added to the table.");
        }
        break;
      case REPLACE_COLUMNS:
        TAlterTableReplaceColsParams replaceColParams = params.getReplace_cols_params();
        alterTableReplaceCols(tbl, replaceColParams.getColumns());
        reloadTableSchema = true;
        addSummary(response, "Table columns have been replaced.");
        break;
      case ADD_PARTITION:
        // Create and add HdfsPartition objects to the corresponding HdfsTable and load
        // their block metadata. Get the new table object with an updated catalog
        // version.
        alterTableAddPartitions(tbl, params.getAdd_partition_params());
        addSummary(response, "New partition has been added to the table.");
        break;
      case DROP_COLUMN:
        TAlterTableDropColParams dropColParams = params.getDrop_col_params();
        alterTableDropCol(tbl, dropColParams.getCol_name());
        reloadTableSchema = true;
        addSummary(response, "Column has been dropped.");
        break;
      case ALTER_COLUMN:
        TAlterTableAlterColParams alterColParams = params.getAlter_col_params();
        alterTableAlterCol(tbl, alterColParams.getCol_name(),
            alterColParams.getNew_col_def());
        reloadTableSchema = true;
        addSummary(response, "Column has been altered.");
        break;
      case DROP_PARTITION:
        TAlterTableDropPartitionParams dropPartParams =
            params.getDrop_partition_params();
        // Drop the partition from the corresponding table. Get the table object
        // with an updated catalog version. If the partition does not exist and
        // "IfExists" is true, null is returned. If "purge" option is specified
        // partition data is purged by skipping Trash, if configured.
        alterTableDropPartition(tbl, dropPartParams.getPartition_set(),
            dropPartParams.isIf_exists(),
            dropPartParams.isPurge());
        addSummary(response,
            "Dropped " + droppedPartitions.size() + " partition(s).");
        break;
      case RENAME_TABLE:
      case RENAME_VIEW:
        Preconditions.checkState(false,
            "RENAME TABLE/VIEW operation has been processed");
        break;
      case SET_FILE_FORMAT:
        TAlterTableSetFileFormatParams fileFormatParams =
            params.getSet_file_format_params();
        reloadFileMetadata = alterTableSetFileFormat(
            tbl, fileFormatParams.getPartition_set(), fileFormatParams.getFile_format());
        if (fileFormatParams.isSetPartition_set()) {
          addSummary(response,
              "Updated " + numUpdatedPartitions.getRef() + " partition(s).");
        } else {
          addSummary(response, "Updated table.");
        }
        break;
      case SET_ROW_FORMAT:
        TAlterTableSetRowFormatParams rowFormatParams =
            params.getSet_row_format_params();
        reloadFileMetadata = alterTableSetRowFormat(tbl,
            rowFormatParams.getPartition_set(), rowFormatParams.getRow_format());
        if (rowFormatParams.isSetPartition_set()) {
          addSummary(response,
              "Updated " + numUpdatedPartitions.getRef() + " partition(s).");
        } else {
          addSummary(response, "Updated table.");
        }
        break;
      case SET_LOCATION:
        TAlterTableSetLocationParams setLocationParams =
            params.getSet_location_params();
        List<TPartitionKeyValue> partitionSpec = setLocationParams.getPartition_spec();
        reloadFileMetadata = alterTableSetLocation(tbl, partitionSpec,
            setLocationParams.getLocation());
        if (partitionSpec == null) {
          addSummary(response, "New location has been set.");
        } else {
          addSummary(response, "New location has been set for the specified partition.");
        }
        break;
      case SET_TBL_PROPERTIES:
        alterTableSetTblProperties(tbl, params.getSet_tbl_properties_params(),
            numUpdatedPartitions);
        reloadTableSchema = true;
        if (params.getSet_tbl_properties_params().isSetPartition_set()) {
          addSummary(response,
              "Updated " + numUpdatedPartitions.getRef() + " partition(s).");
        } else {
          addSummary(response, "Updated table.");
        }
        break;
      case UPDATE_STATS:
        Preconditions.checkState(params.isSetUpdate_stats_params());
        Reference<Long> numUpdatedColumns = new Reference<>(0L);
        alterTableUpdateStats(tbl, params.getUpdate_stats_params(),
            numUpdatedPartitions, numUpdatedColumns, request.debug_action);
        reloadTableSchema = true;
        addSummary(response, "Updated " + numUpdatedPartitions.getRef() +
            " partition(s) and " + numUpdatedColumns.getRef() + " column(s).");
        break;
      case SET_CACHED:
        Preconditions.checkState(params.isSetSet_cached_params());
        String op = params.getSet_cached_params().getCache_op().isSet_cached() ?
            "Cached " : "Uncached ";
        if (params.getSet_cached_params().getPartition_set() == null) {
          reloadFileMetadata =
              alterTableSetCached(tbl, params.getSet_cached_params());
          addSummary(response, op + "table.");
        } else {
          alterPartitionSetCached(tbl, params.getSet_cached_params(),
              numUpdatedPartitions);
          addSummary(response,
              op + numUpdatedPartitions.getRef() + " partition(s).");
        }
        break;
      case RECOVER_PARTITIONS:
        alterTableRecoverPartitions(tbl, request.debug_action);
        addSummary(response, "Partitions have been recovered.");
        break;
      case SET_OWNER:
        Preconditions.checkState(params.isSetSet_owner_params());
        alterTableOrViewSetOwner(tbl, params.getSet_owner_params());
        addSummary(response, "Updated table/view.");
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown ALTER TABLE operation type: " + params.getAlter_type());
    }
  }


  private void alterTableOrViewSetOwner(Table tbl, TAlterTableOrViewSetOwnerParams params)
      throws ImpalaException {
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    oldOwner = msTbl.getOwner();
    oldOwnerType = msTbl.getOwnerType();
    msTbl.setOwner(params.owner_name);
    msTbl.setOwnerType(PrincipalType.valueOf(params.owner_type.name()));

    // A KuduTable is synchronized if it is a managed KuduTable, or an external table
    // with the property of 'external.table.purge' being true.
    boolean isSynchronizedKuduTable = (tbl instanceof KuduTable) &&
        KuduTable.isSynchronizedTable(msTbl);
    boolean altersHMSTable = true;
    if (isSynchronizedKuduTable) {
      boolean isKuduHmsIntegrationEnabled = isKuduHmsIntegrationEnabled(msTbl);
      // We need to update HMS when the integration between Kudu and HMS is not enabled.
      altersHMSTable = !isKuduHmsIntegrationEnabled;
      KuduCatalogOpExecutor.alterSetOwner((KuduTable) tbl, params.owner_name);
    }

    if (altersHMSTable) {
      applyAlterTable(msTbl);
    }
  }

  /**
   * Recover partitions of specified table. Add partitions to metastore which exist in
   * HDFS but not in metastore.
   */
  private void alterTableRecoverPartitions(Table tbl, @Nullable String debugAction)
      throws ImpalaException {
    Preconditions.checkArgument(tbl.isWriteLockedByCurrentThread());
    if (!(tbl instanceof HdfsTable)) {
      throw new CatalogException("Table " + tbl.getFullName() + " is not an HDFS table");
    }
    HdfsTable hdfsTable = (HdfsTable) tbl;
    List<List<String>> partitionsNotInHms = hdfsTable
        .getPathsWithoutPartitions(debugAction);
    if (partitionsNotInHms.isEmpty()) {
      return;
    }

    List<Partition> hmsPartitions = Lists.newArrayList();
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        tbl.getMetaStoreTable().deepCopy();
    TableName tableName = tbl.getTableName();
    for (List<String> partitionSpecValues : partitionsNotInHms) {
      hmsPartitions.add(createHmsPartitionFromValues(
          partitionSpecValues, msTbl, tableName, null));
    }

    String cachePoolName = null;
    Short replication = null;
    Long parentTblCacheDirId =
        HdfsCachingUtil.getCacheDirectiveId(msTbl.getParameters());
    if (parentTblCacheDirId != null) {
      // Inherit the HDFS cache value from the parent table.
      cachePoolName = HdfsCachingUtil.getCachePool(parentTblCacheDirId);
      Preconditions.checkNotNull(cachePoolName);
      replication = HdfsCachingUtil.getCacheReplication(parentTblCacheDirId);
      Preconditions.checkNotNull(replication);
    }

    // Add partitions to metastore.
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      // Apply the updates in batches of 'MAX_PARTITION_UPDATES_PER_RPC'.
      for (List<Partition> hmsSublist :
          Lists.partition(hmsPartitions, MAX_PARTITION_UPDATES_PER_RPC)) {
        // ifNotExists and needResults are true.
        List<Partition> hmsAddedPartitions =
            msClient.getHiveClient().add_partitions(hmsSublist, true, true);
        addedHmsPartitions.addAll(hmsAddedPartitions);
        // Handle HDFS cache.
        if (cachePoolName != null) {
          for (Partition partition : hmsAddedPartitions) {
            long id = HdfsCachingUtil.submitCachePartitionDirective(partition,
                cachePoolName, replication);
            cacheDirIds.add(id);
          }
          // Update the partition metadata to include the cache directive id.
          MetastoreShim.alterPartitions(msClient.getHiveClient(), tableName.getDb(),
              tableName.getTbl(), hmsAddedPartitions);
        }
      }
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "add_partition"), e);
    }
    cacheDirectiveReason = "ALTER TABLE RECOVER PARTITIONS";
  }

  /**
   * Caches or uncaches the HDFS location of the target partitions and updates the
   * partitions' metadata in Hive Metastore Store. If a partition is being cached, the
   * partition properties will have the ID of the cache directive added. If the partition
   * is being uncached, any outstanding cache directive will be dropped and the cache
   * directive ID property key will be cleared.
   */
  private void alterPartitionSetCached(Table tbl,
      TAlterTableSetCachedParams params, Reference<Long> numUpdatedPartitions)
      throws ImpalaException {
    Preconditions.checkArgument(tbl.isWriteLockedByCurrentThread());
    THdfsCachingOp cacheOp = params.getCache_op();
    Preconditions.checkNotNull(cacheOp);
    Preconditions.checkNotNull(params.getPartition_set());
    TableName tableName = tbl.getTableName();
    Preconditions.checkArgument(tbl instanceof HdfsTable);
    List<HdfsPartition> partitions =
        ((HdfsTable) tbl).getPartitionsFromPartitionSet(params.getPartition_set());
    List<HdfsPartition.Builder> modifiedParts = Lists.newArrayList();
    if (cacheOp.isSet_cached()) {
      for (HdfsPartition partition : partitions) {
        // The directive is null if the partition is not cached
        Long directiveId =
            HdfsCachingUtil.getCacheDirectiveId(partition.getParameters());
        HdfsPartition.Builder partBuilder = null;
        short replication = HdfsCachingUtil.getReplicationOrDefault(cacheOp);
        if (directiveId == null) {
          partBuilder = new HdfsPartition.Builder(partition);
          cacheDirIds.add(HdfsCachingUtil.submitCachePartitionDirective(
              partBuilder, cacheOp.getCache_pool_name(), replication));
        } else {
          if (HdfsCachingUtil.isUpdateOp(cacheOp, partition.getParameters())) {
            partBuilder = new HdfsPartition.Builder(partition);
            HdfsCachingUtil.validateCachePool(cacheOp, directiveId, tableName, partition);
            cacheDirIds.add(HdfsCachingUtil.modifyCacheDirective(
                directiveId, partBuilder, cacheOp.getCache_pool_name(), replication));
          }
        }
        if (partBuilder != null) {
          modifiedParts.add(partBuilder);
        }
      }
      cacheDirectiveReason = "ALTER PARTITION SET CACHED";
    } else {
      for (HdfsPartition partition : partitions) {
        if (partition.isMarkedCached()) {
          HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
          HdfsCachingUtil.removePartitionCacheDirective(partBuilder);
          modifiedParts.add(partBuilder);
        }
      }
    }
    try {
      // Do not mark the partitions dirty here since it's done in finally clause.
      bulkAlterPartitions(tbl, modifiedParts, null, UpdatePartitionMethod.NONE);
    } finally {
      ((HdfsTable) tbl).markDirtyPartitions(modifiedParts);
    }
    numUpdatedPartitions.setRef((long) modifiedParts.size());
  }

  /**
   * Caches or uncaches the HDFS location of the target table and updates the table's
   * metadata in Hive Metastore Store. If this is a partitioned table, all uncached
   * partitions will also be cached. The table/partition metadata will be updated to
   * include the ID of each cache directive that was submitted. If the table is being
   * uncached, any outstanding cache directives will be dropped and the cache directive ID
   * property key will be cleared. For partitioned tables, marks the partitions that are
   * affected as 'dirty'. For unpartitioned tables, it returns true to indicate that the
   * file metadata of the table must be reloaded.
   */
  private boolean alterTableSetCached(Table tbl, TAlterTableSetCachedParams params)
      throws ImpalaException {
    Preconditions.checkArgument(tbl.isWriteLockedByCurrentThread());
    THdfsCachingOp cacheOp = params.getCache_op();
    Preconditions.checkNotNull(cacheOp);
    // Alter table params.
    if (!(tbl instanceof HdfsTable)) {
      throw new ImpalaRuntimeException("ALTER TABLE SET CACHED/UNCACHED must target " +
          "an HDFS table.");
    }
    boolean loadFileMetadata = false;
    TableName tableName = tbl.getTableName();
    HdfsTable hdfsTable = (HdfsTable) tbl;
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        tbl.getMetaStoreTable().deepCopy();
    Long cacheDirId = HdfsCachingUtil.getCacheDirectiveId(msTbl.getParameters());
    if (cacheOp.isSet_cached()) {
      short cacheReplication = HdfsCachingUtil.getReplicationOrDefault(cacheOp);
      // If the table was not previously cached (cacheDirId == null) we issue a new
      // directive for this table. If the table was already cached, we validate
      // the pool name and update the cache replication factor if necessary
      if (cacheDirId == null) {
        cacheDirIds.add(HdfsCachingUtil.submitCacheTblDirective(msTbl,
            cacheOp.getCache_pool_name(), cacheReplication));
      } else {
        // Check if the cache directive needs to be changed
        if (HdfsCachingUtil.isUpdateOp(cacheOp, msTbl.getParameters())) {
          HdfsCachingUtil.validateCachePool(cacheOp, cacheDirId, tableName);
          cacheDirIds.add(HdfsCachingUtil.modifyCacheDirective(cacheDirId, msTbl,
              cacheOp.getCache_pool_name(), cacheReplication));
        }
      }

      if (tbl.getNumClusteringCols() > 0) {
        // If this is a partitioned table, submit cache directives for all uncached
        // partitions.
        Collection<? extends FeFsPartition> parts =
            FeCatalogUtils.loadAllPartitions(hdfsTable);
        for (FeFsPartition fePartition : parts) {
          // TODO(todd): avoid downcast
          HdfsPartition partition = (HdfsPartition) fePartition;
          // Only issue cache directives if the data is uncached or the cache directive
          // needs to be updated
          if (!partition.isMarkedCached() ||
              HdfsCachingUtil.isUpdateOp(cacheOp, partition.getParameters())) {
            HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
            try {
              // If the partition was already cached, update the directive otherwise
              // issue new cache directive
              if (!partition.isMarkedCached()) {
                cacheDirIds.add(HdfsCachingUtil.submitCachePartitionDirective(
                    partBuilder, cacheOp.getCache_pool_name(), cacheReplication));
              } else {
                Long directiveId = HdfsCachingUtil.getCacheDirectiveId(
                    partition.getParameters());
                cacheDirIds.add(HdfsCachingUtil.modifyCacheDirective(directiveId,
                    partBuilder, cacheOp.getCache_pool_name(), cacheReplication));
              }
            } catch (ImpalaRuntimeException e) {
              if (partition.isMarkedCached()) {
                LOG.error("Unable to modify cache partition: " +
                    partition.getPartitionName(), e);
              } else {
                LOG.error("Unable to cache partition: " +
                    partition.getPartitionName(), e);
              }
            }

            // Update the partition metadata.
            try {
              applyAlterPartition(tbl, partBuilder);
            } finally {
              ((HdfsTable) tbl).markDirtyPartition(partBuilder);
            }
          }
        }
      } else {
        loadFileMetadata = true;
      }
      cacheDirectiveReason = "ALTER TABLE SET CACHED";
      return loadFileMetadata;
    } else {
      // Uncache the table.
      if (cacheDirId != null) {
        HdfsCachingUtil.removeTblCacheDirective(msTbl);
      }
      // Uncache all table partitions.
      if (tbl.getNumClusteringCols() > 0) {
        Collection<? extends FeFsPartition> parts =
            FeCatalogUtils.loadAllPartitions(hdfsTable);
        for (FeFsPartition fePartition : parts) {
          // TODO(todd): avoid downcast
          HdfsPartition partition = (HdfsPartition) fePartition;
          if (partition.isMarkedCached()) {
            HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
            HdfsCachingUtil.removePartitionCacheDirective(partBuilder);
            try {
              applyAlterPartition(tbl, partBuilder);
            } finally {
              ((HdfsTable) tbl).markDirtyPartition(partBuilder);
            }
          }
        }
      } else {
        loadFileMetadata = true;
      }
    }

    // Update the table metadata.
    applyAlterTable(msTbl);
    return loadFileMetadata;
  }

  /**
   * Alters an existing table's table and/or column statistics. Partitions are updated in
   * batches of size 'MAX_PARTITION_UPDATES_PER_RPC'. This function is used by COMPUTE
   * STATS, COMPUTE INCREMENTAL STATS and ALTER TABLE SET COLUMN STATS. Updates table
   * property 'impala.lastComputeStatsTime' for COMPUTE (INCREMENTAL) STATS, but not for
   * ALTER TABLE SET COLUMN STATS. Returns the number of updated partitions and columns in
   * 'numUpdatedPartitions' and 'numUpdatedColumns', respectively.
   */
  private void alterTableUpdateStats(Table table, TAlterTableUpdateStatsParams params,
      Reference<Long> numUpdatedPartitions, Reference<Long> numUpdatedColumns,
      @Nullable String debugAction)
      throws ImpalaException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    Preconditions.checkState(params.isSetTable_stats() || params.isSetColumn_stats());

    TableName tableName = table.getTableName();
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    if (LOG.isInfoEnabled()) {
      int numPartitions =
          params.isSetPartition_stats() ? params.partition_stats.size() : 0;
      int numColumns =
          params.isSetColumn_stats() ? params.column_stats.size() : 0;
      LOG.info(String.format(
          "Updating stats for table %s: table-stats=%s partitions=%d column-stats=%d",
          tableName, params.isSetTable_stats(), numPartitions, numColumns));
    }

    // Deep copy the msTbl to avoid updating our cache before successfully persisting
    // the results to the metastore.
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        table.getMetaStoreTable().deepCopy();

    // TODO: Transaction committing / aborting seems weird for stat update, but I don't
    //       see other ways to get a new write id (which is needed to update
    //       transactional tables). Hive seems to use internal API for this.
    //       See IMPALA-8865 about plans to improve this.
    TblTransaction tblTxn = null;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      try {
        if (AcidUtils.isTransactionalTable(msTbl.getParameters())) {
          tblTxn = MetastoreShim.createTblTransaction(
              msClient.getHiveClient(), msTbl, -1 /* opens new transaction */);
        }
        alterTableUpdateStatsInner(table, msTbl, params,
            numUpdatedPartitions, numUpdatedColumns, msClient, tblTxn);
        if (tblTxn != null) {
          MetastoreShim.commitTblTransactionIfNeeded(msClient.getHiveClient(), tblTxn);
        }
      } catch (Exception ex) {
        if (tblTxn != null) {
          MetastoreShim.abortTblTransactionIfNeeded(msClient.getHiveClient(), tblTxn);
        }
        throw ex;
      }
    }
    DebugUtils.executeDebugAction(debugAction, DebugUtils.UPDATE_STATS_DELAY);
  }

  private void alterTableUpdateStatsInner(Table table,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      TAlterTableUpdateStatsParams params,
      Reference<Long> numUpdatedPartitions, Reference<Long> numUpdatedColumns,
      MetaStoreClient msClient, TblTransaction tblTxn)
      throws ImpalaException {
    // Update column stats.
    numUpdatedColumns.setRef(0L);
    if (params.isSetColumn_stats()) {
      ColumnStatistics colStats = createHiveColStats(params, table);
      if (colStats.getStatsObjSize() > 0) {
        if (tblTxn != null) {
          MetastoreShim.setTableColumnStatsTransactional(
              msClient.getHiveClient(), msTbl, colStats, tblTxn);
        } else {
          try {
            msClient.getHiveClient().updateTableColumnStatistics(colStats);
          } catch (Exception e) {
            throw new ImpalaRuntimeException(String.format(HMS_RPC_ERROR_FORMAT_STR,
                "updateTableColumnStatistics"), e);
          }
        }
      }
      numUpdatedColumns.setRef((long) colStats.getStatsObjSize());
    }

    // Update partition-level row counts and incremental column stats for
    // partitioned Hdfs tables.
    List<HdfsPartition.Builder> modifiedParts = null;
    if (params.isSetPartition_stats() && table.getNumClusteringCols() > 0) {
      Preconditions.checkState(table instanceof HdfsTable);
      modifiedParts = updatePartitionStats(params, (HdfsTable) table);
      // TODO: IMPALA-10203: avoid reloading modified partitions when updating stats.
      bulkAlterPartitions(table, modifiedParts, tblTxn, UpdatePartitionMethod.MARK_DIRTY);
    }

    if (params.isSetTable_stats()) {
      // Update table row count and total file bytes.
      updateTableStats(params, msTbl);
      // Set impala.lastComputeStatsTime just before alter_table to ensure that it is as
      // accurate as possible.
      Table.updateTimestampProperty(msTbl, HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME);
    }

    // Apply property changes like numRows.
    msTbl.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
    applyAlterTable(msTbl, false, tblTxn);
    numUpdatedPartitions.setRef(0L);
    if (modifiedParts != null) {
      numUpdatedPartitions.setRef((long) modifiedParts.size());
    } else if (params.isSetTable_stats()) {
      numUpdatedPartitions.setRef(1L);
    }
  }


  /**
   * Updates the row count and total file bytes of the given HMS table based on the the
   * update stats parameters.
   */
  private void updateTableStats(TAlterTableUpdateStatsParams params,
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    Preconditions.checkState(params.isSetTable_stats());
    long numRows = params.table_stats.num_rows;
    // Update the table's ROW_COUNT and TOTAL_SIZE parameters.
    msTbl.putToParameters(StatsSetupConst.ROW_COUNT, String.valueOf(numRows));
    if (params.getTable_stats().isSetTotal_file_bytes()) {
      msTbl.putToParameters(StatsSetupConst.TOTAL_SIZE,
          String.valueOf(params.getTable_stats().total_file_bytes));
    }
    // HMS requires this param for stats changes to take effect.
    Pair<String, String> statsTaskParam = MetastoreShim.statsGeneratedViaStatsTaskParam();
    msTbl.putToParameters(statsTaskParam.first, statsTaskParam.second);
  }

  /**
   * Updates the row counts and incremental column stats of the partitions in the given
   * Impala table based on the given update stats parameters. Returns the modified Impala
   * partitions. Row counts for missing or new partitions as a result of concurrent table
   * alterations are set to 0.
   */
  private List<HdfsPartition.Builder> updatePartitionStats(
      TAlterTableUpdateStatsParams params, HdfsTable table) throws ImpalaException {
    Preconditions.checkState(params.isSetPartition_stats());
    List<HdfsPartition.Builder> modifiedParts = Lists.newArrayList();
    // TODO(todd) only load the partitions that were modified in 'params'.
    Collection<? extends FeFsPartition> parts =
        FeCatalogUtils.loadAllPartitions(table);
    for (FeFsPartition fePartition : parts) {
      // TODO(todd): avoid downcast to implementation class
      HdfsPartition partition = (HdfsPartition) fePartition;

      // NULL keys are returned as 'NULL' in the partition_stats map, so don't substitute
      // this partition's keys with Hive's replacement value.
      List<String> partitionValues = partition.getPartitionValuesAsStrings(false);
      TPartitionStats partitionStats = params.partition_stats.get(partitionValues);
      if (partitionStats == null) {
        // No stats were collected for this partition. This means that it was not included
        // in the original computation statements. If the backend does not find any rows
        // for a partition that should be included, it will generate an empty
        // TPartitionStats object.
        if (!params.expect_all_partitions) {
          continue;
        }

        // If all partitions are expected, fill in any missing stats with an empty entry.
        partitionStats = new TPartitionStats();
        if (params.is_incremental) {
          partitionStats.intermediate_col_stats = Maps.newHashMap();
        }
        partitionStats.stats = new TTableStats();
        partitionStats.stats.setNum_rows(0L);
      }

      // Unconditionally update the partition stats and row count, even if the partition
      // already has identical ones. This behavior results in possibly redundant work,
      // but it is predictable and easy to reason about because it does not depend on the
      // existing state of the metadata. See IMPALA-2201.
      long numRows = partitionStats.stats.num_rows;
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Updating stats for partition %s: numRows=%d",
            partition.getValuesAsString(), numRows));
      }
      HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
      PartitionStatsUtil.partStatsToPartition(partitionStats, partBuilder);
      partBuilder.setRowCountParam(numRows);
      // HMS requires this param for stats changes to take effect.
      partBuilder.putToParameters(MetastoreShim.statsGeneratedViaStatsTaskParam());
      partBuilder.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
      modifiedParts.add(partBuilder);
    }
    return modifiedParts;
  }

  /**
   * Create HMS column statistics for the given table based on the give map from column
   * name to column stats. Missing or new columns as a result of concurrent table
   * alterations are ignored.
   */
  private static ColumnStatistics createHiveColStats(
      TAlterTableUpdateStatsParams params, Table table) {
    Preconditions.checkState(params.isSetColumn_stats());
    // Collection of column statistics objects to be returned.
    ColumnStatistics colStats = MetastoreShim.createNewHiveColStats();
    colStats.setStatsDesc(
        new ColumnStatisticsDesc(true, table.getDb().getName(), table.getName()));
    // Generate Hive column stats objects from the update stats params.
    for (Map.Entry<String, TColumnStats> entry : params.getColumn_stats().entrySet()) {
      String colName = entry.getKey();
      Column tableCol = table.getColumn(entry.getKey());
      // Ignore columns that were dropped in the meantime.
      if (tableCol == null) {
        continue;
      }
      // If we know the number of rows in the table, cap NDV of the column appropriately.
      long ndvCap = params.isSetTable_stats() ? params.table_stats.num_rows : -1;
      ColumnStatisticsData colStatsData = ColumnStats.createHiveColStatsData(
          ndvCap, entry.getValue(), tableCol.getType());
      if (colStatsData == null) {
        continue;
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Updating column stats for %s: numDVs=%d numNulls=%d " +
                "maxSize=%d avgSize=%.2f", colName, entry.getValue().getNum_distinct_values(),
            entry.getValue().getNum_nulls(), entry.getValue().getMax_size(),
            entry.getValue().getAvg_size()));
      }
      ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj(colName,
          tableCol.getType().toString().toLowerCase(), colStatsData);
      colStats.addToStatsObj(colStatsObj);
    }
    return colStats;
  }

  /**
   * Appends to the table or partitions property metadata for the given table, replacing
   * the values of any keys that already exist.
   */
  private void alterTableSetTblProperties(Table tbl,
      TAlterTableSetTblPropertiesParams params, Reference<Long> numUpdatedPartitions)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    Map<String, String> properties = params.getProperties();
    Preconditions.checkNotNull(properties);
    if (params.isSetPartition_set()) {
      Preconditions.checkArgument(tbl instanceof HdfsTable);
      List<HdfsPartition> partitions =
          ((HdfsTable) tbl).getPartitionsFromPartitionSet(params.getPartition_set());

      List<HdfsPartition.Builder> modifiedParts = Lists.newArrayList();
      for (HdfsPartition partition : partitions) {
        HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
        switch (params.getTarget()) {
          case TBL_PROPERTY:
            partBuilder.getParameters().putAll(properties);
            break;
          case SERDE_PROPERTY:
            partBuilder.getSerdeInfo().getParameters().putAll(properties);
            break;
          default:
            throw new UnsupportedOperationException(
                "Unknown target TTablePropertyType: " + params.getTarget());
        }
        modifiedParts.add(partBuilder);
      }
      try {
        // Do not mark the partitions dirty here since it's done in finally clause.
        bulkAlterPartitions(tbl, modifiedParts, null, UpdatePartitionMethod.NONE);
      } finally {
        ((HdfsTable) tbl).markDirtyPartitions(modifiedParts);
      }
      numUpdatedPartitions.setRef((long) modifiedParts.size());
    } else {
      // Alter table params.
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      switch (params.getTarget()) {
        case TBL_PROPERTY:
          if (KuduTable.isKuduTable(msTbl)) {
            // If 'kudu.table_name' is specified and this is a synchronized table, rename
            // the underlying Kudu table.
            // TODO(IMPALA-8618): this should be disallowed since IMPALA-5654
            if (properties.containsKey(KuduTable.KEY_TABLE_NAME)
                && !properties.get(KuduTable.KEY_TABLE_NAME).equals(
                msTbl.getParameters().get(KuduTable.KEY_TABLE_NAME))
                && KuduTable.isSynchronizedTable(msTbl)) {
              KuduCatalogOpExecutor.renameTable((KuduTable) tbl,
                  properties.get(KuduTable.KEY_TABLE_NAME));
            }
            msTbl.getParameters().putAll(properties);
            // Validate that the new table properties are valid and that
            // the Kudu table is accessible.
            KuduCatalogOpExecutor.validateKuduTblExists(msTbl);
          } else {
            msTbl.getParameters().putAll(properties);
          }
          break;
        case SERDE_PROPERTY:
          msTbl.getSd().getSerdeInfo().getParameters().putAll(properties);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown target TTablePropertyType: " + params.getTarget());
      }
      applyAlterTable(msTbl);
    }
  }
  /**
   * Changes the HDFS storage location for the given table. This is a metadata only
   * operation, existing table data will not be as part of changing the location.
   */
  private boolean alterTableSetLocation(Table tbl,
      List<TPartitionKeyValue> partitionSpec, String location) throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    boolean reloadFileMetadata = false;
    if (partitionSpec == null) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      if (msTbl.getPartitionKeysSize() == 0) {
        reloadFileMetadata = true;
      }
      msTbl.getSd().setLocation(location);
      applyAlterTable(msTbl);
    } else {
      TableName tableName = tbl.getTableName();
      HdfsPartition partition = catalog_.getHdfsPartition(
          tableName.getDb(), tableName.getTbl(), partitionSpec);
      HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
      partBuilder.setLocation(location);
      try {
        applyAlterPartition(tbl, partBuilder);
      } finally {
        ((HdfsTable) tbl).markDirtyPartition(partBuilder);
      }
    }
    return reloadFileMetadata;
  }

  /**
   * Changes the row format for the given table or partitions. This is a metadata only
   * operation, existing table data will not be converted to the new format. Returns true
   * if the file metadata to be reloaded.
   */
  private boolean alterTableSetRowFormat(Table tbl,
      List<List<TPartitionKeyValue>> partitionSet, TTableRowFormat tRowFormat)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    Preconditions.checkArgument(tbl instanceof HdfsTable);
    boolean reloadFileMetadata = false;
    RowFormat rowFormat = RowFormat.fromThrift(tRowFormat);
    if (partitionSet == null) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      StorageDescriptor sd = msTbl.getSd();
      HiveStorageDescriptorFactory.setSerdeInfo(rowFormat, sd.getSerdeInfo());
      // The prototype partition must be updated if the row format is changed so that new
      // partitions are created with the new file format.
      ((HdfsTable) tbl).setPrototypePartition(msTbl.getSd());
      applyAlterTable(msTbl);
      reloadFileMetadata = true;
    } else {
      List<HdfsPartition> partitions =
          ((HdfsTable) tbl).getPartitionsFromPartitionSet(partitionSet);
      List<HdfsPartition.Builder> modifiedParts = Lists.newArrayList();
      for (HdfsPartition partition : partitions) {
        HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
        HiveStorageDescriptorFactory.setSerdeInfo(rowFormat, partBuilder.getSerdeInfo());
        modifiedParts.add(partBuilder);
      }
      bulkAlterPartitions(tbl, modifiedParts, null, UpdatePartitionMethod.MARK_DIRTY);
      numUpdatedPartitions.setRef((long) modifiedParts.size());
    }
    return reloadFileMetadata;
  }

  /**
   * Changes the file format for the given table or partitions. This is a metadata only
   * operation, existing table data will not be converted to the new format. Returns true
   * if the file metadata to be reloaded.
   */
  private boolean alterTableSetFileFormat(Table tbl,
      List<List<TPartitionKeyValue>> partitionSet, THdfsFileFormat fileFormat)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    boolean reloadFileMetadata = false;
    if (partitionSet == null) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          tbl.getMetaStoreTable().deepCopy();
      setStorageDescriptorFileFormat(msTbl.getSd(), fileFormat);
      // The prototype partition must be updated if the file format is changed so that new
      // partitions are created with the new file format.
      if (tbl instanceof HdfsTable) {
        ((HdfsTable) tbl).setPrototypePartition(msTbl.getSd());
      }
      applyAlterTable(msTbl);
      reloadFileMetadata = true;
    } else {
      Preconditions.checkArgument(tbl instanceof HdfsTable);
      List<HdfsPartition> partitions =
          ((HdfsTable) tbl).getPartitionsFromPartitionSet(partitionSet);
      List<HdfsPartition.Builder> modifiedParts = Lists.newArrayList();
      for (HdfsPartition partition : partitions) {
        modifiedParts.add(new HdfsPartition.Builder(partition).setFileFormat(
            HdfsFileFormat.fromThrift(fileFormat)));
      }
      bulkAlterPartitions(tbl, modifiedParts, null, UpdatePartitionMethod.MARK_DIRTY);
      numUpdatedPartitions.setRef((long) modifiedParts.size());
    }
    return reloadFileMetadata;
  }

  /**
   * Drops existing partitions from the given table in Hive. If a partition is cached, the
   * associated cache directive will also be removed. Also drops the corresponding
   * partitions from its Hdfs table. Returns the table object with an updated catalog
   * version. If none of the partitions exists and "IfExists" is true, null is returned.
   * If purge is true, partition data is permanently deleted. numUpdatedPartitions is used
   * to inform the client how many partitions being dropped in this operation.
   */
  private void alterTableDropPartition(Table tbl,
      List<List<TPartitionKeyValue>> partitionSet,
      boolean ifExists, boolean purge)
      throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    Preconditions.checkNotNull(partitionSet);

    TableName tableName = tbl.getTableName();
    if (!ifExists) {
      Preconditions.checkState(!partitionSet.isEmpty());
    } else {
      if (partitionSet.isEmpty()) {
        LOG.trace(String.format("Ignoring empty partition list when dropping " +
            "partitions from %s because ifExists is true.", tableName));
        return;
      }
    }

    Preconditions.checkArgument(tbl instanceof HdfsTable);
    List<HdfsPartition> parts =
        ((HdfsTable) tbl).getPartitionsFromPartitionSet(partitionSet);

    if (!ifExists && parts.isEmpty()) {
      throw new PartitionNotFoundException(
          "The partitions being dropped don't exist any more");
    }

    PartitionDropOptions dropOptions = PartitionDropOptions.instance();
    dropOptions.purgeData(purge);
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      for (HdfsPartition part : parts) {
        try {
          msClient.getHiveClient().dropPartition(tableName.getDb(), tableName.getTbl(),
              part.getPartitionValuesAsStrings(true), dropOptions);
          droppedPartitions.add(part);
        } catch (NoSuchObjectException e) {
          if (!ifExists) {
            throw new ImpalaRuntimeException(
                String.format(HMS_RPC_ERROR_FORMAT_STR, "dropPartition"), e);
          }
          LOG.trace(
              String.format("Ignoring '%s' when dropping partitions from %s because" +
                  " ifExists is true.", e, tableName));
        }
      }
    } catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "dropPartition"), e);
    }
  }

  /**
   * Changes the column definition of an existing column. This can be used to rename a
   * column, add a comment to a column, or change the datatype of a column.
   */
  private void alterTableAlterCol(Table tbl, String colName,
      TColumn newCol) throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    // Find the matching column name and change it.
    Iterator<FieldSchema> iterator = msTbl.getSd().getColsIterator();
    while (iterator.hasNext()) {
      FieldSchema fs = iterator.next();
      if (fs.getName().toLowerCase().equals(colName.toLowerCase())) {
        fs.setName(newCol.getColumnName());
        Type type = Type.fromThrift(newCol.getColumnType());
        fs.setType(type.toSql().toLowerCase());
        // Don't overwrite the existing comment unless a new comment is given
        if (newCol.getComment() != null) {
          fs.setComment(newCol.getComment());
        }
        String sortByKey = AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS;
        if (msTbl.getParameters().containsKey(sortByKey)) {
          String oldColumns = msTbl.getParameters().get(sortByKey);
          String alteredColumns = MetaStoreUtil.replaceValueInCsvList(oldColumns, colName,
              newCol.getColumnName());
          msTbl.getParameters().put(sortByKey, alteredColumns);
        }
        break;
      }
      if (!iterator.hasNext()) {
        throw new ColumnNotFoundException(String.format(
            "Column name %s not found in table %s.", colName, tbl.getFullName()));
      }
    }
    applyAlterTable(msTbl);
  }

  /**
   * Removes a column from the given table.
   */
  private void alterTableDropCol(Table tbl, String colName) throws ImpalaException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    // Find the matching column name and remove it.
    Iterator<FieldSchema> iterator = msTbl.getSd().getColsIterator();
    while (iterator.hasNext()) {
      FieldSchema fs = iterator.next();
      if (fs.getName().toLowerCase().equals(colName.toLowerCase())) {
        iterator.remove();
        break;
      }
      if (!iterator.hasNext()) {
        throw new ColumnNotFoundException(String.format(
            "Column name %s not found in table %s.", colName, tbl.getFullName()));
      }
    }
    String sortByKey = AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS;
    if (msTbl.getParameters().containsKey(sortByKey)) {
      String oldColumns = msTbl.getParameters().get(sortByKey);
      String alteredColumns = MetaStoreUtil.removeValueFromCsvList(oldColumns, colName);
      msTbl.getParameters().put(sortByKey, alteredColumns);
    }
    applyAlterTable(msTbl);
  }

  /**
   * Returns true if the given alteration type changes the underlying table stored in Kudu
   * in addition to the HMS table.
   */
  private boolean altersKuduTable(TAlterTableType type) {
    return type == TAlterTableType.ADD_COLUMNS
        || type == TAlterTableType.REPLACE_COLUMNS
        || type == TAlterTableType.DROP_COLUMN
        || type == TAlterTableType.ALTER_COLUMN
        || type == TAlterTableType.ADD_DROP_RANGE_PARTITION;
  }

  @Override
  public void doCatalogOperations() throws ImpalaException {

    if (isRenameOperation()) {
      // Rename the table in the Catalog and get the resulting catalog object.
      // ALTER TABLE/VIEW RENAME is implemented as an ADD + DROP.
      TableName oldTblName = tbl.getTableName();
      TableName newTableName = TableName.fromThrift(
          request.getAlter_table_params().getRename_params().getNew_table_name());
      Pair<Table, Table> result =
          catalog_.renameTable(oldTblName.toThrift(), newTableName.toThrift());
      if (result.first == null || result.second == null) {
        // The rename succeeded in the HMS but failed in the catalog cache. The cache is in
        // an inconsistent state, but can likely be fixed by running "invalidate metadata".
        throw new ImpalaRuntimeException(String.format(
            "Table/view rename succeeded in the Hive Metastore, but failed in Impala's " +
                "Catalog Server. Running 'invalidate metadata <tbl>' on the old table name "
                +
                "'%s' and the new table name '%s' may fix the problem.",
            oldTblName.toString(),
            newTableName.toString()));
      }
      catalog_.addVersionsForInflightEvents(false, result.second, newCatalogVersion);
      if (wantMinimalResult) {
        response.result.addToRemoved_catalog_objects(result.first.toInvalidationObject());
        response.result
            .addToUpdated_catalog_objects(result.second.toInvalidationObject());
      } else {
        response.result.addToRemoved_catalog_objects(
            result.first.toMinimalTCatalogObject());
        response.result.addToUpdated_catalog_objects(result.second.toTCatalogObject());
      }
      response.result.setVersion(result.second.getCatalogVersion());
      return;
    }

    // Kudu/Iceberg table reload
    TAlterTableType alterTableType = request.getAlter_table_params().getAlter_type();
    if (altersKuduTable(alterTableType) || altersIcebergTable(alterTableType)) {
      catalogOpExecutor_.loadTableMetadata(tbl, newCatalogVersion, true, true, null,
          "ALTER KUDU TABLE " +
              alterTableType.name());
      addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
      return;
    }

    // HdfsTable alter operations. First add/drop any partitions by the alter operation.
    if (!addedHmsPartitions.isEmpty()) {
      addHdfsPartitions(tbl, addedHmsPartitions);
      tbl.setCatalogVersion(newCatalogVersion);
      // the alter table event is only generated when we add the partition. For
      // instance if not exists clause is provided and the partition is
      // pre-existing there is no alter table event generated. Hence we should
      // only add the versions for in-flight events when we are sure that the
      // partition was really added.
      catalog_.addVersionsForInflightEvents(false, tbl, newCatalogVersion);
      addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
      reloadMetadata = false;
    }

    // drop partitions
    if (!droppedPartitions.isEmpty()) {
      catalog_.dropPartitions(tbl, droppedPartitions);
      tbl.setCatalogVersion(newCatalogVersion);
      // we don't need to add catalog versions in partition's InflightEvents here
      // since by the time the event is received, the partition is already
      // removed from catalog and there is nothing to compare against during
      // self-event evaluation
      addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
      reloadMetadata = false;
    }
    if (!cacheDirIds.isEmpty()) {
      //TODO(Vihang) find out if these ids are unique for each partition?
      // Submit a request to watch these cache directives. The TableLoadingMgr will
      // asynchronously refresh the table metadata once the directives complete.
      Preconditions.checkNotNull(cacheDirectiveReason);
      TableName tableName = tbl.getTableName();
      catalog_.watchCacheDirs(cacheDirIds, tableName.toThrift(), cacheDirectiveReason);
    }

    // alter table set owner
    if (request.getAlter_table_params().isSetSet_owner_params()) {
      Preconditions.checkState(oldOwner != null);
      Preconditions.checkState(oldOwnerType != null);
      TAlterTableOrViewSetOwnerParams params = Preconditions.checkNotNull(request
          .getAlter_table_params().set_owner_params);
      org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable();
      if (catalogOpExecutor_.getAuthzConfig().isEnabled()) {
        catalogOpExecutor_.getAuthzManager()
            .updateTableOwnerPrivilege(params.server_name, msTbl.getDbName(),
                msTbl.getTableName(), oldOwner, oldOwnerType, msTbl.getOwner(),
                msTbl.getOwnerType(), response);
      }
    }
    // Make sure we won't forget finalizing the modification.
    if (tbl.hasInProgressModification()) {
      Preconditions.checkState(reloadMetadata);
    }
    if (reloadMetadata) {
      catalogOpExecutor_.loadTableMetadata(tbl, newCatalogVersion, reloadFileMetadata,
          reloadTableSchema, null,
          "ALTER TABLE " + request.getAlter_table_params().getAlter_type().name());
      // now that HMS alter operation has succeeded, add this version to list of
      // inflight events in catalog table if event processing is enabled
      catalog_.addVersionsForInflightEvents(false, tbl, newCatalogVersion);
      addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
    }
    // Make sure all the modifications are done.
    Preconditions.checkState(!tbl.hasInProgressModification());
  }

  @Override
  public void init() throws ImpalaException {
    TAlterTableParams params = request.getAlter_table_params();
    TableName tableName = TableName.fromThrift(params.getTable_name());
    tbl = catalogOpExecutor_.getExistingTable(tableName.getDb(), tableName.getTbl(),
        "Load for ALTER TABLE");
    if (params.getAlter_type() == TAlterTableType.RENAME_VIEW
        || params.getAlter_type() == TAlterTableType.RENAME_TABLE) {
      TableName newTableName = TableName.fromThrift(
          params.getRename_params().getNew_table_name());
      Preconditions.checkState(!catalog_.isBlacklistedTable(newTableName),
          String.format("Can't rename to blacklisted table name: %s. %s", newTableName,
              BLACKLISTED_DBS_INCONSISTENT_ERR_STR));
    }
    catalogOpExecutor_.tryWriteLock(tbl);
    // Get a new catalog version to assign to the table being altered.
    newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    addCatalogServiceIdentifiers(catalog_, tbl, newCatalogVersion);
    context = tbl.getMetrics().getTimer(Table.ALTER_DURATION_METRIC).time();
    if (!isRenameOperation()) {
      // if this is not a rename operation then we release the catalog lock; else
      // we hold it
      catalog_.getLock().writeLock().unlock();
    }
  }

  @Override
  public void cleanUp() {
    context.stop();
    catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
    // Clear in-progress modifications in case of exceptions.
    tbl.resetInProgressModification();
    tbl.releaseWriteLock();
  }

  public boolean isRenameOperation() {
    TAlterTableParams params = request.getAlter_table_params();
    return params.getAlter_type() == TAlterTableType.RENAME_VIEW
        || params.getAlter_type() == TAlterTableType.RENAME_TABLE;
  }

  private void addHdfsPartitions(Table tbl, List<Partition> partitions)
      throws CatalogException {
    Preconditions.checkNotNull(tbl);
    Preconditions.checkNotNull(partitions);
    if (!(tbl instanceof HdfsTable)) {
      throw new CatalogException("Table " + tbl.getFullName() + " is not an HDFS table");
    }
    HdfsTable hdfsTable = (HdfsTable) tbl;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      List<HdfsPartition> hdfsPartitions = hdfsTable.createAndLoadPartitions(
          msClient.getHiveClient(), partitions);
      for (HdfsPartition hdfsPartition : hdfsPartitions) {
        catalog_.addPartition(hdfsPartition);
      }
    }
  }

}
