package org.apache.impala.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.json.ExtendedJSONMessageFactory;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCreateDatabaseMessage;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Reference;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TTableName;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

/**
 * This class is used to polling metastore for new events at a given frequency. Based on the events
 * which are received during each iteration, it takes certain actions like invalidate table, create
 * table, drop table. The polling of the events is from a given start event id. Subsequently, it
 * updates the last event id until which it has already synced so that next poll is from the last
 * synced event information. Events are requested in batches. The current batch size is constant and
 * set to 1000. In case there is a use-case in the future, we can change the batch size to a
 * configured value. The polling frequency can be configured using the backend config
 * hms_event_polling_frequency_s
 *
 * Following actions are currently taken based on the type of events received from metastore. In
 * case of CREATE_TABLE/CREATE_DATABASE events, a new table/database is created in Catalog
 * respectively. The newly created table/database is Incomplete and should be loaded lazily when
 * needed. In case of DROP_TABLE/DROP_DATABASE event types, the table/database is dropped from
 * catalog if it is present. In case of ALTER_TABLE event, currently the code issues a invalidate
 * table command. There is a special case of ALTER_TABLE event in case of renames, where the old
 * table is removed and a new IncompleteTable is created. This can be potentially improved in the
 * future to do a in-place update of the table in case of renames.
 *
 * In order to function correctly, it assumes that Hive metastore is configured correctly to
 * generate the events. Following configurations should be set in metastore configuration so that
 * events have sufficient information.
 *
 * <code>hive.metastore.notifications.add.thrift.objects</code> should be set to <code>true</code>
 * so that event messages contain thrift object and can be used to create new objects in Catalog
 * <code>hive.metastore.alter.notifications.basic</code> should be set to <code>true</code> so that
 * all the needed ALTER events are generated.
 *
 * Currently, in case of ADD_PARTITION/ALTER_PARTITION/DROP_PARTITION events, it issues invalidate
 * on the table. This can be optimized by issuing add/refresh/drop the partition at the Table level
 *
 * INDEX and FUNCTION (CREATE, ALTER, DROP) events are currently ignored.
 * TODO: INSERT_EVENT are currently ignored IMPALA-7971
 * TODO: add logic to detect self-events  IMPALA-7972
 */
class MetastoreEventsProcessor {

  static final String METASTORE_NOTIFICATIONS_ADD_THRIFT_OBJECTS =
      "hive.metastore.notifications.add.thrift.objects";

  // keeps track of the last event id which we have synced to
  private long lastSyncedEventId_;

  private final CatalogServiceCatalog catalog_;
  private static final Logger LOG = Logger.getLogger(MetastoreEventsProcessor.class);

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("MetastoreEventsProcessor")
          .build());

  // Use ExtendedJSONMessageFactory to deserialize the event messages. ExtendedJSONMessageFactory
  // adds additional information over JSONMessageFactory so that events are compatible with Sentry
  //TODO this should be moved to JSONMessageFactory when Sentry switches to JSONMessageFactory
  private static final MessageFactory messageFactory = ExtendedJSONMessageFactory.getInstance();
  private static MetastoreEventsProcessor INSTANCE;

  private static final int EVENTS_BATCH_SIZE = 1000;

  private MetastoreEventsProcessor(CatalogServiceCatalog catalog, long startSyncFromId,
      long pollingFrequencyInSec) {
    Preconditions.checkNotNull(catalog);
    this.catalog_ = catalog;
    //this assumes that when MetastoreEventsProcessor is created, catalogD just came
    //up in which case it already is going to do a full-sync with HMS
    //TODO figure out if CatalogD sync with HMS is in-process or completed so that we know
    //if we need to ignore some of events which are already applied in catalogD
    lastSyncedEventId_ = startSyncFromId;
    // If the polling frequency is set to 0 don't schedule
    Preconditions.checkState(pollingFrequencyInSec >= 0);
    if (pollingFrequencyInSec > 0) {
      scheduleAtFixedDelayRate(pollingFrequencyInSec);
    }
  }

  /**
   * Schedules the daemon thread of the given frequency. It is important to note that this method
   * schedules with FixedDelay instead of FixedRate. The reason it is scheduled at a fixedDelay is
   * to make sure that we don't pile up the pending tasks in case each polling operation is taking
   * longer than the given frequency. Because of the fixed delay, the new poll operation is scheduled
   * at the time when previousPoll operation completes + givenDelayInSec
   *
   * @param pollingFrequencyInSec Number of seconds at which the polling needs to be done
   */
  void scheduleAtFixedDelayRate(long pollingFrequencyInSec) {
    Preconditions.checkState(pollingFrequencyInSec > 0);
    scheduler.scheduleWithFixedDelay(() -> {
      try {
        processHMSNotificationEvents();
      } catch (ImpalaException e) {
        LOG.warn(String.format("Unexpected exception %s received while processing metastore events",
            e.getMessage()));
      }
    }, pollingFrequencyInSec, pollingFrequencyInSec, TimeUnit.SECONDS);
  }

  /**
   * Gets the current notification event id from metastore. This is used to compare with the last
   * event_id to determine if we need poll events from metastore
   */
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

  /**
   * Utility exception class to be thrown for errors during event processing
   */
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

  /**
   * This method issues a request to Hive Metastore if needed, based on the current event id in
   * metastore and the last synced event_id. Events are fetched in fixed sized batches.
   */
  @VisibleForTesting
  void processHMSNotificationEvents() throws ImpalaException {
    lastSyncedEventId_ = lastSyncedEventId_ == -1 ? getCurrentNotificationID() : lastSyncedEventId_;
    if (lastSyncedEventId_ == -1) {
      LOG.warn("Unable to fetch current notification event id. Cannot sync with metastore");
      return;
    }
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      CurrentNotificationEventId currentNotificationEventId = msClient.getHiveClient()
          .getCurrentNotificationEventId();
      long currentEventId = currentNotificationEventId.getEventId();
      if (currentEventId > lastSyncedEventId_) {
        NotificationEventResponse response = msClient.getHiveClient()
            .getNextNotification(lastSyncedEventId_, EVENTS_BATCH_SIZE, null);
        LOG.info("Received " + response.getEvents().size() + " events. Start event id : "
            + lastSyncedEventId_);
        for (NotificationEvent event : response.getEvents()) {
          // update the lastSyncedEventId before processing the event itself
          // in case there are errors while processing the event. Otherwise, the sync thread
          // will be stuck forever at this bad event and keep throwing exception until catalogD
          // is restarted
          lastSyncedEventId_ = event.getEventId();
          processEvent(event);
        }
      }
    } catch (TException e) {
      throw new MetastoreNotificationException(
          "Unable to fetch notifications from metastore. Last synced event id is "
              + lastSyncedEventId_, e);
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
        //TODO may be do a direct update using partition object here. See IMPALA-7973
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
        if (!processRenameTableEvent(event)) {
          invalidateCatalogTable(table);
        }
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
        //TODO need to fix HIVE bug while de-serializing alter_database event message
        /*if (db != null) {
          msDb = MetastoreEventsProcessor.getDatabaseFromMessage(event);
          db.setMSDb(msDb);
        }*/
        break;
      //TODO do we care about indexes?
      case "CREATE_INDEX":
      case "ALTER_INDEX":
      case "DROP_INDEX":
        //TODO function events are currently not supported
      case "CREATE_FUNCTION":
      case "ALTER_FUNCTION":
      case "DROP_FUNCTION":
      default: {
        LOG.warn(String
            .format("Unsupported event id %d of type %s received. Ignoring ...", event.getEventId(),
                event.getEventType()));
      }
    }
  }

  /**
   * If the ALTER_TABLE event is due a table rename, this method removes the old table and creates
   * a new table with the new name.
   * //TODO Check if we can rename the existing table in-place
   * @param event
   * @return true if the event was rename event and remove of old table name and adding of table with the new name is successful.
   * Returns false, if the alter event is not for rename operation
   * @throws MetastoreNotificationException
   */
  private boolean processRenameTableEvent(NotificationEvent event)
      throws MetastoreNotificationException {
    JSONAlterTableMessage alterTableMessage = (JSONAlterTableMessage) messageFactory
        .getDeserializer().getAlterTableMessage(event.getMessage());
    try {
      org.apache.hadoop.hive.metastore.api.Table oldTable = alterTableMessage
          .getTableObjBefore();
      org.apache.hadoop.hive.metastore.api.Table newTable = alterTableMessage
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
      catalog_.addTable(newTable.getDbName(), newTable.getTableName());
      return true;
    } catch (Exception e) {
      throw new MetastoreNotificationException(e);
    }
  }

  /**
   * Deserializes the event message and create a database object out of it.
   * @param event a CREATE_DATABASE or ALTER_DATABASE event
   * @return Database object deserialized from event message
   * @throws MetastoreNotificationException
   */
  private static Database getDatabaseFromMessage(NotificationEvent event)
      throws MetastoreNotificationException {
    Preconditions.checkState(
        "CREATE_DATABASE" .equalsIgnoreCase(event.getEventType()) || "ALTER_DATABASE"
            .equalsIgnoreCase(event.getEventType()));
    Database msDb = null;
    try {
      if ("CREATE_DATABASE" .equalsIgnoreCase(event.getEventType())) {
        JSONCreateDatabaseMessage createDatabaseMessage = (JSONCreateDatabaseMessage) messageFactory
            .getDeserializer().getCreateDatabaseMessage(event.getMessage());
        msDb = createDatabaseMessage.getDatabaseObject();
      } else if ("ALTER_DATABASE" .equalsIgnoreCase(event.getEventType())) {
        JSONAlterDatabaseMessage alterDatabaseMessage = (JSONAlterDatabaseMessage) messageFactory
            .getDeserializer().getAlterDatabaseMessage(event.getMessage());
        msDb = alterDatabaseMessage.getDbObjAfter();
        Database msDbBefore = alterDatabaseMessage.getDbObjBefore();
        if (!checkSupportedCasesForAlterDatabaseEvent(msDbBefore, msDb)) {
          throw new MetastoreNotificationException(String.format(
              "Unsupported alter_database event received. Event id %d, Database before : %s, Database after : %s",
              event.getEventId(), msDbBefore, msDb));
        }
      }
      if (msDb == null) {
        throw new MetastoreNotificationException(
            String.format(
                "Database object is null in the event id %d : event message %s. "
                    + "This could be a metastore configuration problem. "
                    + "Check if %s is set to true in metastore configuration",
                event.getEventId(), event.getMessage(),
                METASTORE_NOTIFICATIONS_ADD_THRIFT_OBJECTS));
      }
      return msDb;
    } catch (Exception e) {
      throw new MetastoreNotificationException(e);
    }
  }

  /**
   * We support only the following alter_database conditions. 1. If the database parameters are
   * changed 2. If the database owner is changed 3. If the database location is changed
   */
  private static boolean checkSupportedCasesForAlterDatabaseEvent(Database msDbBefore, Database msDbAfter) {
    Preconditions.checkNotNull(msDbBefore);
    Preconditions.checkNotNull(msDbAfter);
    return !StringUtils.equalsIgnoreCase(msDbAfter.getOwnerName(), msDbBefore.getOwnerName())
        || !StringUtils.equalsIgnoreCase(msDbAfter.getLocationUri(), msDbBefore.getLocationUri())
        || !(msDbAfter.isSetParameters() && msDbAfter.getParameters()
        .equals(msDbBefore.getParameters()));
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
    if (INSTANCE == null) {
      INSTANCE = new MetastoreEventsProcessor(catalog, startSyncFromId,
          config.getHMSPollingFrequencyInSeconds());
    }
    return INSTANCE;
  }
}
