package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.Transaction;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.TransactionKeepalive.HeartbeatContext;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.IcebergCatalogOpExecutor;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TTruncateParams;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.AcidUtils.TblTransaction;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Truncate a table by deleting all files in its partition directories, and dropping all
 * column and table statistics. Acquires a table lock to protect against concurrent
 * table modifications. TODO truncate specified partitions.
 */
public class TruncateTableOperation extends CatalogOperation {

  private static final Logger LOG = LoggerFactory.getLogger(TruncateTableOperation.class);
  private final TTruncateParams params;
  private final TableName tblName;
  private long newCatalogVersion;
  private Table table;

  public TruncateTableOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response,
      CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
    params = Preconditions.checkNotNull(ddlExecRequest.truncate_params);
    tblName = TableName.fromThrift(params.getTable_name());
  }

  @Override
  protected boolean takeDdlLock() {
    return false;
  }

  @Override
  protected void doHmsOperations() throws ImpalaException {
    try {
      if (AcidUtils.isTransactionalTable(table.getMetaStoreTable().getParameters())) {
        truncateTransactionalTable(table);
      } else if (table instanceof FeIcebergTable) {
        truncateIcebergTable(table);
      } else {
        truncateNonTransactionalTable(table);
      }
    } catch (Exception e) {
      String fqName = tblName.getDb() + "." + tblName.getTbl();
      throw new CatalogException(String.format("Failed to truncate table: %s.\n" +
          "Table may be in a partially truncated state.", fqName), e);
    }
    Preconditions.checkState(newCatalogVersion > 0);
    addSummary(response, "Table has been truncated.");
  }

  /**
   * Truncates a transactional table. It creates new empty base directories in all
   * partitions of the table. That way queries started earlier can still read a valid
   * snapshot version of the data. HMS's cleaner should remove obsolete directories later.
   * After that empty directory creation it removes stats-related parameters of the table
   * and its partitions.
   */
  private void truncateTransactionalTable(Table table)
      throws ImpalaException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      IMetaStoreClient hmsClient = msClient.getHiveClient();
      HeartbeatContext ctx = new HeartbeatContext(
          String.format("Truncate table %s.%s", tblName.getDb(), tblName.getTbl()),
          System.nanoTime());
      try (Transaction txn = catalog_.openTransaction(hmsClient, ctx)) {
        Preconditions.checkState(txn.getId() > 0);
        // We need to release catalog table lock here, because HMS Acid table lock
        // must be locked in advance to avoid dead lock.
        // TODO(Vihang) We have to do catalog operations here because we release and
        // retake the lock and hence we need to get a new catalogVersion too.
        table.releaseWriteLock();
        //TODO: if possible, set DataOperationType to something better than NO_TXN.
        catalog_.lockTableInTransaction(tblName.getDb(), tblName.getTbl(), txn,
            DataOperationType.NO_TXN, ctx);
        catalogOpExecutor_.tryWriteLock(table, "truncating");
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
          List<Partition> hmsPartitions =
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
    } catch (Exception e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "truncateTable"), e);
    }
  }

  /**
   * Helper method to check if the database which table belongs to is a source of Hive
   * replication. We cannot rely on the Db object here due to eventual nature of cache
   * updates.
   */
  private boolean isTableBeingReplicated(IMetaStoreClient metastoreClient,
      HdfsTable tbl) throws CatalogException {
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
    String dbName = tbl.getDb().getName();
    try {
      Database db = metastoreClient.getDatabase(dbName);
      if (!db.isSetParameters()) {
        return false;
      }
      return org.apache.commons.lang.StringUtils
          .isNotEmpty(db.getParameters().get("repl.source.for"));
    } catch (TException tException) {
      throw new CatalogException(
          String.format("Could not determine if the table %s is a replication source",
              tbl.getFullName()), tException);
    }
  }

  /**
   * Creates new empty base directories for an ACID table. The directories won't be really
   * empty, they will contain the "empty" file. It's needed because
   * FileSystemUtil.listFiles() doesn't see empty directories. See IMPALA-8739.
   *
   * @param partitions the partitions in which we create new directories.
   * @param writeId    the write id of the new base directory.
   * @throws IOException
   */
  private void createEmptyBaseDirectories(
      Collection<? extends FeFsPartition> partitions, long writeId) throws IOException {
    for (FeFsPartition part : partitions) {
      Path partPath = new Path(part.getLocation());
      FileSystem fs = FileSystemUtil.getFileSystemForPath(partPath);
      String baseDirStr =
          part.getLocation() + Path.SEPARATOR + "base_" + String.valueOf(writeId);
      fs.mkdirs(new Path(baseDirStr));
      String emptyFile = baseDirStr + Path.SEPARATOR + "empty";
      fs.create(new Path(emptyFile)).close();
    }
  }

  private void truncateIcebergTable(Table table)
      throws Exception {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    Preconditions.checkState(table instanceof FeIcebergTable);
    FeIcebergTable iceTable = (FeIcebergTable) table;
    dropColumnStats(table);
    dropTableStats(table);
    IcebergCatalogOpExecutor.truncateTable(iceTable);
  }

  private void truncateNonTransactionalTable(Table table)
      throws Exception {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
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
        return;
      }
    }
    Collection<? extends FeFsPartition> parts = FeCatalogUtils
        .loadAllPartitions(hdfsTable);
    for (FeFsPartition part : parts) {
      FileSystemUtil.deleteAllVisibleFiles(new Path(part.getLocation()));
    }
    dropColumnStats(table);
    dropTableStats(table);
  }

  @Override
  protected void doCatalogOperations() throws ImpalaException {
    catalogOpExecutor_
        .loadTableMetadata(table, newCatalogVersion, true, true, null, "TRUNCATE");
    addTableToCatalogUpdate(table, wantMinimalResult, response.result);
  }

  @Override
  protected void before() throws ImpalaException {
    try {
      table = catalogOpExecutor_
          .getExistingTable(tblName.getDb(), tblName.getTbl(),
              "Load for TRUNCATE TABLE");
    } catch (TableNotFoundException e) {
      if (params.if_exists) {
        addSummary(response, "Table does not exist.");
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
    catalogOpExecutor_.tryWriteLock(table, "truncating");
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    Preconditions.checkState(catalog_.getLock().isWriteLockedByCurrentThread());
    newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
  }

  @Override
  protected void after() throws ImpalaException {
    catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
    if (table.isWriteLockedByCurrentThread()) {
      table.releaseWriteLock();
    }
  }
}
