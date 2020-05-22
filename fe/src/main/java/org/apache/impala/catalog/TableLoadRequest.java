package org.apache.impala.catalog;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.impala.thrift.TTableName;

/**
 * The table loading request provides all the information need to a TableLoader to trigger
 * a load of a given table. Callers can optionally provide a {@link
 * org.apache.hadoop.hive.common.ValidWriteIdList} in case the table is transactional.
 */
public class TableLoadRequest {

  // the dbname for the table to be loaded
  private final String dbname_;
  // the name of the table to be loaded
  private final String tblName_;
  // reason for which we requesting a load of table. Used for logging purposes
  private final String reason_;
  // if this is a transactional table, optionally provide a ValidWriteIdList
  // this is used to compare against an already loaded table and return the
  // if the loaded table's writeIdList is >= the value of validWriteList_
  // can be set to null we this is not a transactional table or we want to force
  // a load.
  @Nullable
  private final ValidWriteIdList validWriteIdList_;

  private TableLoadRequest(String dbname, String tblname, String reason,
    ValidWriteIdList writeIdList) {
    this.dbname_ = Preconditions.checkNotNull(dbname);
    this.tblName_ = Preconditions.checkNotNull(tblname);
    this.reason_ = Preconditions.checkNotNull(reason);
    this.validWriteIdList_ = writeIdList;
  }

  public static TableLoadRequest create(TTableName tblname, String reason) {
    return new TableLoadRequest(tblname.db_name, tblname.table_name, reason, null);
  }

  public static TableLoadRequest create(String dbname, String tblName, String reason,
    ValidWriteIdList validWriteIdList) {
    return new TableLoadRequest(dbname, tblName, reason, validWriteIdList);
  }

  /**
   * @return the dbname for this request
   */
  public String getDbname() {
    return dbname_;
  }

  /**
   * @return the tblname for this request
   */
  public String getTblName() {
    return tblName_;
  }

  /**
   * @return the reason for triggering the load. For example, "needed by coordinator".
   */
  public String getReason() {
    return reason_;
  }

  /**
   * If the table is expected to be a transactional table, provides an optional
   * ValidWriteIdList which represents the snapshot view of the caller. Can be null if
   * caller doesn't know if the table is transactional or not.
   * @return
   */
  @Nullable
  public ValidWriteIdList getValidWriteIdList() {
    return validWriteIdList_;
  }
}
