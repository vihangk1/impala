package org.apache.impala.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.json.ExtendedJSONMessageFactory;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCreateDatabaseMessage;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Reference;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TTableName;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

class MetastoreEventsProcessor {

  static final String METASTORE_NOTIFICATIONS_ADD_THRIFT_OBJECTS =
      "hive.metastore.notifications.add.thrift.objects";
  private long lastSyncedEventId_;
  private final CatalogServiceCatalog catalog_;
  private final int eventBatchSize_;
  private static final Logger LOG = Logger.getLogger(MetastoreEventsProcessor.class);
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("MetastoreEventsProcessor")
          .build());

  private static final MessageFactory messageFactory = ExtendedJSONMessageFactory.getInstance();
  private static MetastoreEventsProcessor INSTANCE;

  private MetastoreEventsProcessor(CatalogServiceCatalog catalog, long startSyncFromId,
      BackendConfig backendConfig) {
    Preconditions.checkNotNull(catalog);
    //TODO get interval and batchsize from config
    this.catalog_ = catalog;
    this.eventBatchSize_ = 1000;
    //this assumes that when MetastoreEventsProcessor is created, catalogD just came
    //up in which case it already is going to do a full-sync with HMS
    //TODO figure out if CatalogD sync with HMS is in-process or completed so that we know
    //if we need to ignore some of events which are already applied in catalogD
    lastSyncedEventId_ = startSyncFromId;
    scheduler.scheduleWithFixedDelay(() -> {
      try {
        processHMSNotificationEvents();
      } catch (ImpalaException e) {
        LOG.warn(String.format("Unexpected exception %s received while processing metastore events",
            e.getMessage()));
      }
    }, 2, 2, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  void disableSchedulingForTests() {
    scheduler.shutdownNow();
  }

  private long getCurrentNotificationID() {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      CurrentNotificationEventId currentNotificationEventId = msClient.getHiveClient()
          .getCurrentNotificationEventId();
      return currentNotificationEventId.getEventId();
    } catch (TException e) {
      LOG.warn("Unable to fetch current event id from metastore", e);
    }
    return -1;
  }

  static class MetastoreNotificationException extends ImpalaException {

    public MetastoreNotificationException(String msg, Throwable cause) {
      super(msg, cause);
    }

    public MetastoreNotificationException(String msg) {
      super(msg);
    }

    public MetastoreNotificationException(Exception e) {
      super(e);
    }
  }

  @VisibleForTesting
  void processHMSNotificationEvents() throws ImpalaException {
    lastSyncedEventId_ = lastSyncedEventId_ == -1 ? getCurrentNotificationID() : lastSyncedEventId_;
    if (lastSyncedEventId_ == -1) {
      LOG.warn("Unable to fetch current notification event id. Cannot sync with metastore");
      return;
    }
    LOG.info("Syncing metastore events from event id " + lastSyncedEventId_);
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      CurrentNotificationEventId currentNotificationEventId = msClient.getHiveClient()
          .getCurrentNotificationEventId();
      long currentEventId = currentNotificationEventId.getEventId();
      if (currentEventId > lastSyncedEventId_) {
        NotificationEventResponse response = msClient.getHiveClient()
            .getNextNotification(lastSyncedEventId_, eventBatchSize_, null);
        LOG.info("Received " + response.getEvents().size() + " events");
        for (NotificationEvent event : response.getEvents()) {
          processEvent(event);
          lastSyncedEventId_ = event.getEventId();
        }
      }
    } catch (TException e) {
      throw new MetastoreNotificationException("Unable to fetch notifications from metastore", e);
    }
  }

  private void processEvent(NotificationEvent event) throws ImpalaException {
    LOG.info("Processing event " + event.getEventId() + " : " + event.getEventType() + " : " + event
        .getTableName());
    String tblName = event.getTableName();
    String dbName = event.getDbName();
    Db db = catalog_.getDb(dbName);
    Table table = (db != null && tblName != null) ? db.getTable(tblName) : null;
    switch (event.getEventType()) {
      case "ADD_PARTITION":
      case "ALTER_PARTITION":
      case "DROP_PARTITION":
        //TODO is refresh better here?
        //TODO may be do a direct update using partition object. But catalogD may need block
        //locations anyway. So is it worth it?
        invalidateCatalogTable(table);
        break;
      case "CREATE_TABLE":
        catalog_.addTable(dbName, tblName);
        LOG.info(String.format("Added table %s.%s", dbName, tblName));
        break;
      case "ALTER_TABLE":
        //in case of table level alters from external systems it is better to do a full invalidate
        //eg. this could be due to as simple as adding a new parameter or a full blown adding
        //or changing column type
        // detect the special where a table is renamed
        removeOldTableIfRenamed(event);
        invalidateCatalogTable(table);
        break;
      case "DROP_TABLE":
        catalog_.removeTable(dbName, tblName);
        LOG.info(String.format("Removed table %s.%s", dbName, tblName));
        break;
      case "CREATE_DATABASE":
        Database msDb = MetastoreEventsProcessor.getDatabaseFromMessage(event);
        Db oldDb = catalog_.addDb(dbName, msDb);
        StringBuilder msg = new StringBuilder();
        if (oldDb != null) {
          msg.append("Replaced existing database ");
        } else {
          msg.append("Added a new database ");
        }
        msg.append(dbName);
        LOG.info(msg.toString());
        break;
      case "DROP_DATABASE":
        if (db != null) {
          catalog_.removeDb(dbName);
        }
        break;
      case "ALTER_DATABASE":
        // in case of alter_database there operations are limited to the database and not cascaded
        // down to the tables within the tables. Hence we don't need to remove the database and
        // add again
        if (db != null) {
        }
        break;
      //TODO does Impala care about indexes?
      case "CREATE_INDEX":
      case "ALTER_INDEX":
      case "DROP_INDEX":
        //TODO function events are currently not supported
      case "CREATE_FUNCTION":
      case "ALTER_FUNCTION":
      case "DROP_FUNCTION":
      default: {
        LOG.warn(String
            .format("Ignoring event id %d of type %s", event.getEventId(), event.getEventType()));
      }
    }
  }

  private boolean removeOldTableIfRenamed(NotificationEvent event) throws MetastoreNotificationException {
    JSONAlterTableMessage createDatabaseMessage = (JSONAlterTableMessage) messageFactory
        .getDeserializer().getAlterTableMessage(event.getMessage());
    try {
      org.apache.hadoop.hive.metastore.api.Table oldTable = createDatabaseMessage
          .getTableObjBefore();
      org.apache.hadoop.hive.metastore.api.Table newTable = createDatabaseMessage
          .getTableObjAfter();
      if (oldTable.getDbName().equalsIgnoreCase(newTable.getDbName()) && oldTable.getTableName()
          .equalsIgnoreCase(newTable.getTableName())) {
        // table is not renamed, nothing to do
        return false;
      }
      // table was renamed, remove the old table
      LOG.info(String
          .format("Found that %s.%s table was renamed. Removing it", oldTable.getDbName(),
              oldTable.getTableName()));
      catalog_.removeTable(oldTable.getDbName(), oldTable.getTableName());
      return true;
    } catch (Exception e) {
      throw new MetastoreNotificationException(e);
    }
  }

  @VisibleForTesting
  MessageFactory getMessageFactory() {
    return messageFactory;
  }

  private static Database getDatabaseFromMessage(NotificationEvent event)
      throws MetastoreNotificationException {
    JSONCreateDatabaseMessage createDatabaseMessage = (JSONCreateDatabaseMessage) messageFactory
        .getDeserializer().getCreateDatabaseMessage(event.getMessage());
    Database msDb = null;
    try {
      msDb = createDatabaseMessage.getDatabaseObject();
      if (msDb == null) {
        throw new MetastoreNotificationException(
            String.format(
                "Database object is null in the event id %d : event message %s. "
                    + "This could be a metastore configuration problem. "
                    + "Check if hive.metastore.notifications.add.thrift.objects is set in metastore configuration",
                event.getEventId(), event.getMessage()));
      }
      return msDb;
    } catch (Exception e) {
      throw new MetastoreNotificationException(e);
    }
  }

  private void invalidateCatalogTable(Table table) {
    if (table == null) {
      return;
    }
    TTableName tTableName = table.getTableName().toThrift();
    Reference<Boolean> tblWasRemoved = new Reference<>();
    Reference<Boolean> dbWasAdded = new Reference<>();
    catalog_.invalidateTable(tTableName, tblWasRemoved, dbWasAdded);
    LOG.info(
        "Table " + table.getTableName() + " in database " + table.getDb().getName()
            + " invalidated because the table was recreated in metastore");
  }

  public static synchronized MetastoreEventsProcessor create(CatalogServiceCatalog catalog,
      long startSyncFromId, BackendConfig config) {
    //TODO get config values from backendConf
    if (INSTANCE == null) {
      INSTANCE = new MetastoreEventsProcessor(catalog, startSyncFromId, config);
    }
    return INSTANCE;
  }
}
