package org.apache.impala.catalog;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.messaging.json.ExtendedJSONMessageFactory;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCreateDatabaseMessage;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Reference;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TTableName;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

class MetastoreEventsProcessor {

  private static final String MESSAGE_DESERIALIZER_CLASS = ExtendedJSONMessageFactory.class
      .getName();
  private long lastSyncedEventId_ = -1;
  private final CatalogServiceCatalog catalog_;
  private final int eventBatchSize_;
  private static final Logger LOG = Logger.getLogger(MetastoreEventsProcessor.class);
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("MetastoreEventsProcessor")
          .build());

  private static MetastoreEventsProcessor INSTANCE;

  private MetastoreEventsProcessor(CatalogServiceCatalog catalog, int batchSize) {
    Preconditions.checkNotNull(catalog);
    Preconditions.checkArgument(batchSize > 0);
    this.catalog_ = catalog;
    this.eventBatchSize_ = batchSize;
    //this assumes that when MetastoreEventsProcessor is created, catalogD just came
    //up in which case it already is going to do a full-sync with HMS
    //TODO figure out if CatalogD sync with HMS is in-process or completed so that we know
    //if we need to ignore some of events which are already applied in catalogD
    lastSyncedEventId_ = getCurrentNotificationID();
    scheduler.scheduleWithFixedDelay(() -> {
      try {
        processHMSNotificationEvents();
      } catch (ImpalaException e) {
        LOG.warn(String.format("Unexpected exception %s received while processing metastore events", e.getMessage()));
      }
    }, 2, 2, TimeUnit.SECONDS);
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
  }

  private void processHMSNotificationEvents() throws ImpalaException {
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
        if (table != null) {
          //this is possible if the catalog has a stale version of the table
          //for example someone dropped and recreated the table in HMS in the time window
          //of before instantiating MetastoreEventsProcessor and after catalogD loaded the older version
          //table
          //TODO need to have a solid mechanism (like version numbers) to make sure that table
          //we have is different than the table we received from HMS
          invalidateCatalogTable(table);
        } else {
          Table incompleteTbl = IncompleteTable.createUninitializedTable(db, tblName);
          incompleteTbl.setCatalogVersion(catalog_.incrementAndGetCatalogVersion());
          db.addTable(incompleteTbl);
          LOG.info("Added Table " + incompleteTbl.getFullName());
        }
        break;
      case "ALTER_TABLE":
        invalidateCatalogTable(table);
        break;
      case "DROP_TABLE":
        catalog_.removeTable(dbName, tblName);
        break;
      case "CREATE_DATABASE":
        if (db != null) {
          //TODO this directly removes the object from the cache. Need to update ImpalaDs via statestore
          catalog_.removeDb(dbName);
        }
        //TODO need to backport database object from message patch in HMS
        JSONCreateDatabaseMessage createDatabaseMessage = (JSONCreateDatabaseMessage) ExtendedJSONMessageFactory
            .getInstance()
            .getDeserializer().getCreateDatabaseMessage(event.getMessage());
        Database msDb;
        try {
          msDb = createDatabaseMessage.getDatabaseObject();
          if (msDb == null) {
            throw new MetastoreNotificationException(
                String.format("Database object is null in the event id %d : event message %s. This could be a metastore configuration problem.",
                event.getEventId(), event.getMessage()));
          }
          catalog_.addDb(dbName, msDb);
        } catch (Exception e) {
          throw new MetastoreNotificationException("Unable to get database object from notification", e);
        }
        /*try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
          msDb = msClient.getHiveClient().getDatabase(dbName);
          catalog_.addDb(dbName, msDb);
        } catch (NoSuchObjectException e) {
          LOG.info(String.format("Ignoring event %d of type %s. Object not found in metastore", event.getEventId(), event.getEventType()));
        } catch (TException e) {
          throw new MetastoreNotificationException("Failed to get the database " + dbName, e);
        }*/
        break;
      case "DROP_DATABASE":
        if (db != null) {
          //TODO this directly removes the object from the cache. Need to update ImpalaDs via statestore
          catalog_.removeDb(dbName);
        }
        break;
      case "ALTER_DATABASE":
        //TODO need to investigate how can we handle alter db
      case "CREATE_INDEX":
      case "ALTER_INDEX":
      case "DROP_INDEX":
        //TODO does Impala care about indexes?
      case "CREATE_FUNCTION":
        //TODO function events are current disabled in DbNotificationListener
        /*JSONCreateFunctionMessage createFunctionMessage = (JSONCreateFunctionMessage) ExtendedJSONMessageFactory
            .getInstance().getDeserializer().getCreateFunctionMessage(event.getMessage());*/
        //createFunctionMessage.get
        //catalog_
      case "ALTER_FUNCTION":
      case "DROP_FUNCTION":
      default: {
        LOG.warn(String
            .format("Ignoring event id %d of type %s", event.getEventId(), event.getEventType()));
      }
    }
  }

  private void invalidateCatalogTable(Table table) {
    if (table == null) return;
    TTableName tTableName = table.getTableName().toThrift();
    Reference<Boolean> tblWasRemoved = new Reference<>();
    Reference<Boolean> dbWasAdded = new Reference<>();
    catalog_.invalidateTable(tTableName, tblWasRemoved, dbWasAdded);
    LOG.info(
        "Table " + table.getTableName() + " in database " + table.getDb().getName()
            + " invalidated because the table was recreated in metastore");
  }

  public static synchronized MetastoreEventsProcessor get(CatalogServiceCatalog catalog,
      BackendConfig config) {
    //TODO get config values from backendConf
    if (INSTANCE == null) {
      INSTANCE = new MetastoreEventsProcessor(catalog, 1000);
    }
    return INSTANCE;
  }
}
