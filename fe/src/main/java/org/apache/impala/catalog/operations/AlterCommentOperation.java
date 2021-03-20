package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import java.util.Iterator;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.ColumnNotFoundException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.KuduCatalogOpExecutor;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnName;
import org.apache.impala.thrift.TCommentOnParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDdlType;

public class AlterCommentOperation extends CatalogOperation {

  private Db db;
  private TCommentOnParams params;
  private long newCatalogVersion;
  private Database msDb;
  private Table tbl;
  private TableName tableName;
  private TColumnName columnName;

  public AlterCommentOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response,
      CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
    params = Preconditions.checkNotNull(ddlExecRequest.getComment_on_params());
  }

  @Override
  protected boolean takeDdlLock() {
    return false;
  }

  @Override
  protected void doHmsOperations() throws ImpalaException {
    String comment = params.getComment();
    if (params.isSetDb()) {
      // comment on db
      Preconditions.checkNotNull(db);
      msDb = db.getMetaStoreDb().deepCopy();
      addCatalogServiceIdentifiers(msDb, catalog_.getCatalogServiceId(),
          newCatalogVersion);
      msDb.setDescription(comment);
      try {
        applyAlterDatabase(msDb);
      } catch (ImpalaRuntimeException e) {
        throw e;
      }
    } else if (params.getTable_name() != null) {
      // comment on table
      Preconditions.checkNotNull(tbl);
      Preconditions.checkState(tbl.isWriteLockedByCurrentThread());
      Preconditions.checkState(newCatalogVersion > 0);
      addCatalogServiceIdentifiers(tbl, catalog_.getCatalogServiceId(),
          newCatalogVersion);
      org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable()
          .deepCopy();
      if (comment == null) {
        msTbl.getParameters().remove("comment");
      } else {
        msTbl.getParameters().put("comment", comment);
      }
      applyAlterTable(msTbl);
    } else {
      // comment on column
      Preconditions.checkNotNull(params.getColumn_name());
      String columnName = params.getColumn_name().getColumn_name();
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
    }
  }

  /**
   * Find the matching column name in the iterator and update its comment. Return true if
   * found; false otherwise.
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

  @Override
  protected void doCatalogOperations() throws ImpalaException {
    if (params.isSetDb()) {
      Db updatedDb = catalog_.updateDb(msDb);
      addDbToCatalogUpdate(updatedDb, wantMinimalResult, response.result);
      // now that HMS alter operation has succeeded, add this version to list of inflight
      // events in catalog database if event processing is enabled
      catalog_.addVersionsForInflightEvents(db, newCatalogVersion);
      addSummary(response, "Updated database.");
    } else {
      String reason;
      String summary;
      if (params.getTable_name() != null) {
        reason = "ALTER COMMENT";
        boolean isView = tbl.getMetaStoreTable().getTableType().equalsIgnoreCase(
            TableType.VIRTUAL_VIEW.toString());
        summary = String.format("Updated %s.", (isView) ? "view" : "table");
      } else {
        reason = "ALTER COLUMN COMMENT";
        summary = "Column has been altered.";
      }
      catalogOpExecutor_
          .loadTableMetadata(tbl, newCatalogVersion, false, false, null, reason);
      addTableToCatalogUpdate(tbl, wantMinimalResult, response.result);
      addSummary(response, summary);
    }
  }

  @Override
  protected void before() throws ImpalaException {
    Preconditions.checkState(params.getDb() != null || params.getTable_name() != null
        || params.getColumn_name() != null);
    if (params.getDb() != null) {
      String dbName = params.getDb();
      db = catalog_.getDb(dbName);
      if (db == null) {
        throw new CatalogException("Database: " + dbName + " does not exist.");
      }
      catalogOpExecutor_.tryLock(db, "altering the comment");
      // Get a new catalog version to assign to the database being altered.
      newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
      catalog_.getLock().writeLock().unlock();
    } else if (params.getTable_name() != null || params.getColumn_name() != null) {
      tableName = TableName.fromThrift(params.getTable_name());
      String reason = params.getTable_name() != null ? "Load for ALTER COMMENT"
          : "Load for ALTER COLUMN COMMENT";
      tbl = catalogOpExecutor_
          .getExistingTable(tableName.getDb(), tableName.getTbl(), reason);
      catalogOpExecutor_.tryWriteLock(tbl);
      newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
      catalog_.getLock().writeLock().unlock();
    }
  }

  @Override
  protected void after() throws ImpalaException {
    if (db != null && db.getLock().isHeldByCurrentThread()) {
      db.getLock().unlock();
    } else if (tbl != null && tbl.isWriteLockedByCurrentThread()) {
      tbl.releaseWriteLock();
    }
  }
}
