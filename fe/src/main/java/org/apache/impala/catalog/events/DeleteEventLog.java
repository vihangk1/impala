package org.apache.impala.catalog.events;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.HdfsTable;

public class DeleteEventLog {

  private SortedMap<Long, Object> eventLog_;
  // key format for databases "DB:catalogName.DbName"
  private static final String DB_KEY_FORMAT_STR = "DB:%s.%s";
  // key format for tables "TBL:catalogName.DbName.tblName"
  private static final String TBL_KEY_FORMAT_STR = "TBL:%s.%s.%s";
  // key format for tables "TBL:catalogName.DbName.tblName"
  // TODO Add catalog name here.
  // key format for partitions "PART:FullTblName.partName"
  private static final String PART_KEY_FORMAT_STR = "PART:%s.%s";

  public DeleteEventLog() {
    eventLog_ = new TreeMap<>();
  }

  public static String getPartitionKey(HdfsTable hdfsTable, List<String> partValues) {
    return String.format(PART_KEY_FORMAT_STR, hdfsTable.getFullName(),
        FileUtils.makePartName(hdfsTable.getClusteringColNames(), partValues));
  }

  public synchronized void addRemovedObject(long eventId, Object value) {
    Preconditions.checkNotNull(value);
    eventLog_.put(eventId, value);
  }

  public synchronized boolean wasRemovedAfter(long eventId, Object value) {
    Preconditions.checkNotNull(value);
    return keyExistsAfterEventId(eventId, value);
  }

  private boolean keyExistsAfterEventId(long eventId, Object key) {
    for (Object objectName : eventLog_.tailMap(eventId + 1).values()) {
      if (key.equals(objectName)) {
        return true;
      }
    }
    return false;
  }

  public synchronized void garbageCollect(long eventId) {
    if (!eventLog_.isEmpty() && eventLog_.firstKey() <= eventId) {
      eventLog_ = new TreeMap<>(eventLog_.tailMap(eventId + 1));
    }
  }

  public static String getDbKey(String catalogName, String dbName) {
    return String
        .format(DB_KEY_FORMAT_STR, catalogName, dbName).toLowerCase();
  }

  public static String getTblKey(String catalogName, String dbName, String tblName) {
    return String.format(TBL_KEY_FORMAT_STR, catalogName, dbName, tblName).toLowerCase();
  }

  public static String getKey(Database database) {
    return getDbKey(database.getCatalogName(), database.getName());
  }

  public static String getKey(Table tbl) {
    return String
        .format(TBL_KEY_FORMAT_STR, tbl.getCatName(), tbl.getDbName(), tbl.getTableName())
        .toLowerCase();
  }
}
