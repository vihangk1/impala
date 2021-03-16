package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartition.Builder;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDropStatsParams;
import org.apache.impala.util.AcidUtils;
import org.apache.thrift.TException;

/**
 * Drops all table and column stats from the target table in the HMS and
 * updates the Impala catalog. Throws an ImpalaException if any errors are
 * encountered as part of this operation. Acquires a lock on the modified table
 * to protect against concurrent modifications.
 */
public class DropStatsOperation extends CatalogOperation {
  private Table table;
  private TDropStatsParams params;
  private long newCatalogVersion;
  private final List<HdfsPartition.Builder> partitionBuilders = Lists.newArrayList();

  public DropStatsOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response,
      CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
  }

  @Override
  protected boolean takeDdlLock() {
    return false;
  }

  @Override
  protected void doHmsOperations() throws ImpalaException {
    if (params.getPartition_set() == null) {
      // TODO: Report the number of updated partitions/columns to the user?
      // TODO: bulk alter the partitions.
      dropColumnStats(table);
      dropTableStats(table);
    } else {
      HdfsTable hdfsTbl = (HdfsTable) table;
      List<HdfsPartition> partitions =
          hdfsTbl.getPartitionsFromPartitionSet(params.getPartition_set());
      if (partitions.isEmpty()) {
        addSummary(response, "No partitions found for table.");
        return;
      }

      for (HdfsPartition partition : partitions) {
        if (partition.getPartitionStatsCompressed() != null) {
          HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(partition);
          partBuilder.dropPartitionStats();
          applyAlterPartition(table, partBuilder);
          partitionBuilders.add(partBuilder);
        }
      }
    }
  }

  @Override
  protected void doCatalogOperations() throws ImpalaException {
    if (!partitionBuilders.isEmpty()) {
      HdfsTable hdfsTbl = (HdfsTable) table;
      for (HdfsPartition.Builder partBuilders : partitionBuilders) {
        hdfsTbl.updatePartition(partBuilders);
      }
    }
    catalogOpExecutor_.loadTableMetadata(table, newCatalogVersion, /*reloadFileMetadata=*/false,
        /*reloadTableSchema=*/true, /*partitionsToUpdate=*/null, "DROP STATS");
    addTableToCatalogUpdate(table, wantMinimalResult, response.result);
    addSummary(response, "Stats have been dropped.");
  }

  @Override
  protected void before() throws ImpalaException {
    params = Preconditions.checkNotNull(request.drop_stats_params);
    table = catalogOpExecutor_.getExistingTable(params.getTable_name().getDb_name(),
        params.getTable_name().getTable_name(), "Load for DROP STATS");
    Preconditions.checkNotNull(table);
    // There is no transactional HMS API to drop stats at the moment (HIVE-22104).
    Preconditions.checkState(!AcidUtils.isTransactionalTable(
        table.getMetaStoreTable().getParameters()));

    catalogOpExecutor_.tryWriteLock(table, "dropping stats");
    newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
  }

  @Override
  protected void after() {
    UnlockWriteLockIfErronouslyLocked();
    table.releaseWriteLock();
  }

  /**
   * Drops all column stats from the table in the HMS. Returns the number of columns
   * that were updated as part of this operation.
   */
  private int dropColumnStats(Table table) throws ImpalaRuntimeException {
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
   * Drops all table and partition stats from this table in the HMS.
   * Partitions are updated in batches of MAX_PARTITION_UPDATES_PER_RPC. Returns
   * the number of partitions updated as part of this operation, or 1 if the table
   * is unpartitioned.
   */
  private int dropTableStats(Table table) throws ImpalaException {
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
}
