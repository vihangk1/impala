package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDropStatsParams;
import org.apache.impala.util.AcidUtils;

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
}
