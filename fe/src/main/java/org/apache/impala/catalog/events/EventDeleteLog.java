package org.apache.impala.catalog.events;

import com.google.common.base.Preconditions;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;

public class EventDeleteLog {
  private SortedMap<Long, String> deleteLog_;
  // key format for databases "DB:catalogName.DbName"
  private static final String DB_KEY_FORMAT_STR = "DB:%s.%s";
  // key format for tables "TBL:catalogName.DbName.TblName"
  private static final String TBL_KEY_FORMAT_STR = "TBL:%s.%s.%s";
  public EventDeleteLog() {
    deleteLog_ = new TreeMap<>();
  }

  public synchronized void addRemovedObject(long eventId, String name) {
    Preconditions.checkNotNull(name);
    deleteLog_.put(eventId, name);
  }

  public synchronized boolean wasRemovedAfter(long eventId, Database msDb) {
    Preconditions.checkNotNull(msDb);
    String name = getKey(msDb);
    for (String objectName : deleteLog_.tailMap(eventId).values()) {
      if (name.equals(objectName)) {
        return true;
      }
    }
    return false;
  }

  public synchronized void garbageCollect(long eventId) {
    if (!deleteLog_.isEmpty() && deleteLog_.firstKey() < eventId) {
      deleteLog_ = new TreeMap<>(deleteLog_.tailMap(eventId));
    }
  }

  public static String getKey(Database database) {
    return String
        .format(DB_KEY_FORMAT_STR, database.getCatalogName(), database.getName())
        .toLowerCase();
  }

  public static String getKey(Table tbl) {
    return String.format(TBL_KEY_FORMAT_STR, tbl.getCatName(), tbl.getDbName(),
        tbl.getTableName()).toLowerCase();
  }
}
