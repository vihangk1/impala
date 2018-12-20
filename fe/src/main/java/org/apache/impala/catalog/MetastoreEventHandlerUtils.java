// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.catalog;

import com.google.common.base.Preconditions;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropTableMessage;
import org.apache.impala.analysis.TableName;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.thrift.TTableName;
import org.apache.log4j.Logger;

/**
 * Util class which provides Metastore event handlers for various event types. Also
 * provides a MetastoreEventHandlerFactory to get or create the event handler instances
 * for a given event type
 */
public class MetastoreEventHandlerUtils {
  // metastore table event types
  public static final String CREATE_TABLE_EVENT_TYPE = "CREATE_TABLE";
  public static final String DROP_TABLE_EVENT_TYPE = "DROP_TABLE";
  public static final String ALTER_TABLE_EVENT_TYPE = "ALTER_TABLE";

  // metastore database event types
  public static final String CREATE_DATABASE_EVENT_TYPE = "CREATE_DATABASE";
  public static final String DROP_DATABASE_EVENT_TYPE = "DROP_DATABASE";
  public static final String ALTER_DATABASE_EVENT_TYPE = "ALTER_DATABASE";

  // metastore partition event types
  public static final String ADD_PARTITION_EVENT_TYPE = "ADD_PARTITION";
  public static final String DROP_PARTITION_EVENT_TYPE = "DROP_PARTITION";
  public static final String ALTER_PARTITION_EVENT_TYPE = "ALTER_PARTITION";

  // default key to map to unknown event types
  public static final String DEFAULT_EVENT_HANDLER_KEY = "default";

  /**
   * Factory class to create various EventHandlers. Keeps a cache of existing
   * eventHandlers so that they are reused and only one instance of any eventHandler is
   * created
   */
  public static class MetastoreEventHandlerFactory {
    // catalog service instance to be used for creating eventHandlers
    private final CatalogServiceCatalog catalog_;

    // keep a copy of ignoring event handler and table invalidating event handler to
    // reuse for multiple event types
    private final MetastoreEventHandler ignoringEventHandler_;
    private final MetastoreEventHandler tableInvalidatingEventHandler_;

    // cache of existing eventHandlers which can be re-used when instantiating a
    // handler for a given event type. Initial capacity is fixed to 15 since we don't
    // expect to have more than 15 supported event types
    private final ConcurrentHashMap<String, MetastoreEventHandler> eventhandlers_ =
        new ConcurrentHashMap<>(15);

    public MetastoreEventHandlerFactory(CatalogServiceCatalog catalog) {
      this.catalog_ = Preconditions.checkNotNull(catalog);
      this.ignoringEventHandler_ = new IgnoringEventHandler(catalog_);
      this.tableInvalidatingEventHandler_ = new TableInvalidatingEventHandler(catalog_);
    }

    /**
     * Atomically creates or returns an existing instance of event handler for the given
     * event type. If the event type is unknown, returns a IgnoringEventHandler
     */
    public MetastoreEventHandler getOrCreate(String eventType) {
      Preconditions.checkNotNull(eventType);
      switch (eventType) {
        case CREATE_TABLE_EVENT_TYPE:
          return eventhandlers_.computeIfAbsent(
              CREATE_TABLE_EVENT_TYPE, value -> new CreateTableEventHandler(catalog_));
        case DROP_TABLE_EVENT_TYPE:
          return eventhandlers_.computeIfAbsent(
              DROP_TABLE_EVENT_TYPE, value -> new DropTableEventHandler(catalog_));
        case ALTER_TABLE_EVENT_TYPE:
          return eventhandlers_.computeIfAbsent(
              ALTER_TABLE_EVENT_TYPE, value -> new AlterTableEventHandler(catalog_));
        case CREATE_DATABASE_EVENT_TYPE:
          return eventhandlers_.computeIfAbsent(CREATE_DATABASE_EVENT_TYPE,
              value -> new CreateDatabaseEventHandler(catalog_));
        case DROP_DATABASE_EVENT_TYPE:
          return eventhandlers_.computeIfAbsent(
              DROP_DATABASE_EVENT_TYPE, value -> new DropDatabaseEventHandler(catalog_));
        case ALTER_DATABASE_EVENT_TYPE:
          // alter database events are currently ignored
          return eventhandlers_.computeIfAbsent(
              ALTER_DATABASE_EVENT_TYPE, value -> ignoringEventHandler_);
        case ADD_PARTITION_EVENT_TYPE:
          // add partition events triggers invalidate table currently
          return eventhandlers_.computeIfAbsent(
              ADD_PARTITION_EVENT_TYPE, value -> tableInvalidatingEventHandler_);
        case DROP_PARTITION_EVENT_TYPE:
          // drop partition events triggers invalidate table currently
          return eventhandlers_.computeIfAbsent(
              DROP_PARTITION_EVENT_TYPE, value -> tableInvalidatingEventHandler_);
        case ALTER_PARTITION_EVENT_TYPE:
          // alter partition events triggers invalidate table currently
          return eventhandlers_.computeIfAbsent(
              ALTER_PARTITION_EVENT_TYPE, value -> tableInvalidatingEventHandler_);
        default:
          // ignore all the unknown events by creating a ignoringEventHandler
          return eventhandlers_.computeIfAbsent(
              DEFAULT_EVENT_HANDLER_KEY, value -> ignoringEventHandler_);
      }
    }
  }

  /**
   * Abstract base class for all the Metastore Event Handlers. Provides some util methods
   * which can be used in the sub-classes.
   */
  public static abstract class MetastoreEventHandler {
    // use this.getClass to make sure that subclass names are visible in the logs
    protected final Logger LOG = Logger.getLogger(this.getClass().getName());
    // catalogServiceCatalog instance used to update the state based on event info
    protected final CatalogServiceCatalog catalog_;

    private MetastoreEventHandler(CatalogServiceCatalog catalog) {
      this.catalog_ = catalog;
    }

    /**
     * Processes a given NotificationEvent. All sub-classes should implement this method
     * to handle various event types
     *
     * @throws MetastoreNotificationException when there are errors while processing
     *     events
     * @throws CatalogException when there are errors when there are errors while
     *     applying the events to the catalog
     */
    protected abstract void process(NotificationEvent event)
        throws MetastoreNotificationException, CatalogException;

    /**
     * Utility method to validate a given NotificationEvent given the expected event type.
     * Parses the event information and returns the dbName and tblName pair
     */
    protected Pair<String, String> validateAndInit(
        String expectedType, NotificationEvent event) {
      LOG.debug(String.format("Processing event %d of type %s on table %s",
          event.getEventId(), event.getEventType(), event.getTableName()));
      Preconditions.checkArgument(expectedType.equalsIgnoreCase(event.getEventType()),
          String.format("Unexpected event type %s. Expected %s", event.getEventType(),
              expectedType));
      String dbName = Preconditions.checkNotNull(event.getDbName());
      // CREATE_DATABASE event does not have tblName set, all the other event types should
      // have tblName
      String tblName = null;
      if (!event.getEventType().equalsIgnoreCase("CREATE_DATABASE")
          && !event.getEventType().equalsIgnoreCase("DROP_DATABASE")) {
        tblName = Preconditions.checkNotNull(event.getTableName());
      }
      return new Pair<>(dbName, tblName);
    }

    /**
     * Util method to issue invalidate on a given table on the catalog. This method
     * atomically invalidates the table if it exists in the catalog. No-op if the table
     * does not exist
     */
    protected boolean invalidateCatalogTable(String dbName, String tblName) {
      return catalog_.invalidateTableIfExists(dbName, tblName) != null;
    }

    /**
     * Util method to parse the event object and return the fully qualified table name
     * which is of the format dbName.tblName
     */
    protected String getFullyQualifiedTblName(NotificationEvent event) {
      return new TableName(event.getDbName(), event.getTableName()).toString();
    }
  }

  /**
   * Event handler for CREATE_TABLE event type
   */
  private static class CreateTableEventHandler extends MetastoreEventHandler {

    /**
     * Prevent instantiation from outside should use MetastoreEventHandlerFactory instead
     */
    private CreateTableEventHandler(CatalogServiceCatalog catalog) { super(catalog); }

    /**
     * If the table provided in the catalog does not exist in the catalog, this event
     * handler will create it. If the table in the catalog already exists, this handler
     * relies of the creationTime of the Metastore Table to resolve the conflict. If the
     * catalog table's creation time is less than creationTime of the table from the
     * event, it will be overridden. Else, it will ignore the event
     */
    @Override
    public void process(NotificationEvent event) throws MetastoreNotificationException {
      Pair<String, String> dbAndTblName = validateAndInit(CREATE_TABLE_EVENT_TYPE, event);
      // check if the table exists already. This could happen in corner cases of the
      // table being dropped and recreated with the same name or in case this event is
      // a self-event (see description of self-event in the class documentation above)
      String dbName = dbAndTblName.first;
      String tblName = dbAndTblName.second;
      Reference<Boolean> dbWasFound = new Reference<>();
      Reference<Boolean> tblAlreadyExists = new Reference<>();
      Table addedTable =
          catalog_.addTableIfNotExists(dbName, tblName, dbWasFound, tblAlreadyExists);
      if (!dbWasFound.getRef()) {
        // if table was not added, it could be due to the fact that the db did not
        // exist in the catalog cache. This could only happen if the previous
        // create_database event for this table errored out
        throw new MetastoreNotificationException(String.format(
            "Unable to add table while processing event id %d for table %s because the "
                + "database doesn't exist. This could be due to a previous error while "
                + "processing CREATE_DATABASE event for the database %s",
            event.getEventId(), getFullyQualifiedTblName(event), event.getDbName()));
      } else if (tblAlreadyExists.getRef()) {
        LOG.warn(
            String.format("Not adding the table %s from the event %d since it already "
                    + "exists in catalog",
                tblName, event.getEventId()));
      } else {
        Preconditions.checkState(addedTable instanceof IncompleteTable);
        LOG.info(String.format("Added a incomplete table %s.%s after processing event "
                + "if %d of the type %s",
            dbName, tblName, event.getEventId(), event.getEventType()));
      }
    }
  }

  /**
   * EventHandler for ALTER_TABLE event type
   */
  private static class AlterTableEventHandler extends MetastoreEventHandler {
    /**
     * Prevent instantiation from outside should use MetastoreEventHandlerFactory instead
     */
    private AlterTableEventHandler(CatalogServiceCatalog catalog) { super(catalog); }

    /**
     * If the ALTER_TABLE event is due a table rename, this method removes the old table
     * and creates a new table with the new name. Else, this just issues a invalidate
     * table on the tblName from the event//TODO Check if we can rename the existing table
     * in-place
     */
    @Override
    public void process(NotificationEvent event) throws MetastoreNotificationException {
      Pair<String, String> dbAndTblName = validateAndInit("ALTER_TABLE", event);
      // in case of table level alters from external systems it is better to do a full
      // invalidate  eg. this could be due to as simple as adding a new parameter or a
      // full blown adding  or changing column type
      // detect the special where a table is renamed
      JSONAlterTableMessage alterTableMessage =
          (JSONAlterTableMessage) MetastoreEventsProcessor.getMessageFactory()
              .getDeserializer()
              .getAlterTableMessage(event.getMessage());
      try {
        String dbName = dbAndTblName.first;
        String tblName = dbAndTblName.second;
        org.apache.hadoop.hive.metastore.api.Table oldTable =
            alterTableMessage.getTableObjBefore();
        org.apache.hadoop.hive.metastore.api.Table newTable =
            alterTableMessage.getTableObjAfter();
        if (oldTable.getDbName().equalsIgnoreCase(newTable.getDbName())
            && oldTable.getTableName().equalsIgnoreCase(newTable.getTableName())) {
          // table is not renamed, need to invalidate
          if (!invalidateCatalogTable(dbName, tblName)) {
            LOG.debug(String.format("Table %s.%s does not need to be "
                    + "invalidated since "
                    + "it does not exist anymore",
                dbName, tblName));
          } else {
            LOG.info(String.format("Table %s.%s is invalidated after processing event "
                    + "id %d of the type %s",
                dbName, tblName, event.getEventId(), event.getEventType()));
          }
          return;
        }
        // table was renamed, remove the old table
        LOG.info(String.format("Found that %s.%s table was renamed. Removing it",
            oldTable.getDbName(), oldTable.getTableName()));
        TTableName oldTTableName =
            new TTableName(oldTable.getDbName(), oldTable.getTableName());
        TTableName newTTableName =
            new TTableName(newTable.getDbName(), newTable.getTableName());

        // atomically rename the old table to new table
        Pair<Table, Table> result = catalog_.renameTable(oldTTableName, newTTableName);

        if (result == null) {
          throw new MetastoreNotificationException(String.format("Could not rename "
                  + "table %s to %s "
                  + "while processing event %d",
              qualify(oldTTableName), qualify(newTTableName), event.getEventId()));
        } else if (result.first == null) {
          throw new MetastoreNotificationException(String.format("Could not remove old "
                  + "table while processing rename event id %d to rename table %s to %s",
              event.getEventId(), qualify(oldTTableName), qualify(newTTableName)));
        } else if (result.second == null) {
          throw new MetastoreNotificationException(String.format("Could not add new "
                  + "table while processing rename event id %d to rename table %s to %s",
              event.getEventId(), qualify(oldTTableName), qualify(newTTableName)));
        }
      } catch (Exception e) {
        throw new MetastoreNotificationException(e);
      }
    }

    private String qualify(TTableName tTableName) {
      return new TableName(tTableName.db_name, tTableName.table_name).toString();
    }
  }

  /**
   * EventHandler for the DROP_TABLE event type
   */
  private static class DropTableEventHandler extends MetastoreEventHandler {
    /**
     * Prevent instantiation from outside should use MetastoreEventHandlerFactory instead
     */
    private DropTableEventHandler(CatalogServiceCatalog catalog) { super(catalog); }

    /**
     * Process the drop table event type. If the table from the event doesn't exist in the
     * catalog, ignore the event. If the table exists in the catalog, compares the
     * createTime of the table in catalog with the createTime of the table from the event
     * and remove the catalog table if there is a match. If the catalog table is a
     * incomplete table it is removed as well.
     */
    @Override
    public void process(NotificationEvent event) throws MetastoreNotificationException {
      Pair<String, String> dbAndTblName = validateAndInit(DROP_TABLE_EVENT_TYPE, event);
      try {
        JSONDropTableMessage dropTableMessage =
            (JSONDropTableMessage) MetastoreEventsProcessor.getMessageFactory()
                .getDeserializer()
                .getDropTableMessage(event.getMessage());
        org.apache.hadoop.hive.metastore.api.Table droppedTable =
            dropTableMessage.getTableObj();
        if (droppedTable == null) {
          throw new MetastoreNotificationException(
              String.format("Table object is null in the event id %d : event message %s. "
                      + "This could be a metastore configuration problem. "
                      + "Check if %s is set to true in metastore configuration",
                  event.getEventId(), event.getMessage(),
                  MetastoreEventsProcessor
                      .METASTORE_NOTIFICATIONS_ADD_THRIFT_OBJECTS_CONFIG_KEY));
        }
        Reference<Boolean> tblWasFound = new Reference<>();
        Reference<Boolean> tblMatched = new Reference<>();
        Table removedTable =
            catalog_.removeTableIfExists(droppedTable, tblWasFound, tblMatched);
        if (removedTable == null) {
          if (!tblMatched.getRef()) {
            LOG.warn(String.format("Table %s from the event id %d was not removed from "
                    + "catalog since the creation time of the table did not match",
                dbAndTblName.second));
          } else if (!tblWasFound.getRef()) {
            LOG.debug(String.format("Table %s from event id %d was not removed since it "
                    + "did not exist in catalog.",
                dbAndTblName.second));
          }
        } else {
          LOG.info(String.format("Removed table %s.%s after processing event id %d of "
                  + "type %s",
              dbAndTblName.first, dbAndTblName.second, event.getEventId(),
              event.getEventType()));
        }
      } catch (Exception ex) {
        throw new MetastoreNotificationException("Unable to retrieve table object from "
                + "the drop table message",
            ex);
      }
    }
  }

  /**
   * Event handler for CREATE_DATABASE event type
   */
  private static class CreateDatabaseEventHandler extends MetastoreEventHandler {
    /**
     * Prevent instantiation from outside should use MetastoreEventHandlerFactory instead
     */
    private CreateDatabaseEventHandler(CatalogServiceCatalog catalog) { super(catalog); }

    /**
     * Processes the create database event by adding the Db object from the event if it
     * does not exist in the catalog already. //TODO we should compare the creationTime of
     * the Database in catalog with the Database in the event to make sure we are ignoring
     * only catalog has the latest Database object. This will be added after HIVE-21077 is
     * fixed and available
     */
    @Override
    public void process(NotificationEvent event) throws MetastoreNotificationException {
      Pair<String, String> dbAndTblName =
          validateAndInit(CREATE_DATABASE_EVENT_TYPE, event);
      JSONCreateDatabaseMessage createDatabaseMessage =
          (JSONCreateDatabaseMessage) MetastoreEventsProcessor.getMessageFactory()
              .getDeserializer()
              .getCreateDatabaseMessage(event.getMessage());
      Database msDb;
      try {
        msDb = createDatabaseMessage.getDatabaseObject();
      } catch (Exception e) {
        throw new MetastoreNotificationException(e);
      }
      if (msDb == null) {
        throw new MetastoreNotificationException(String.format(
            "Database object is null in the event id %d : event message %s. "
                + "This could be a metastore configuration problem. "
                + "Check if %s is set to true in metastore configuration",
            event.getEventId(), event.getMessage(),
            MetastoreEventsProcessor
                .METASTORE_NOTIFICATIONS_ADD_THRIFT_OBJECTS_CONFIG_KEY));
      }
      // if the database already exists in catalog, by definition, it is a later version
      // of the database since metastore will not allow it be created if it was already
      // existing at the time of creation. In such case, it is safe to assume that the
      // already existing database in catalog is a later version with the same name and
      // this event can be ignored
      if (catalog_.addDbIfNotExists(dbAndTblName.first, msDb)) {
        LOG.info(String.format("Successfully added database %s", dbAndTblName.first));
      } else {
        LOG.info(String.format("Database %s already exists. Ignoring the event id %d",
            dbAndTblName.first, event.getEventId()));
      }
    }
  }

  /**
   * Event handler for the DROP_DATABASE event
   */
  private static class DropDatabaseEventHandler extends MetastoreEventHandler {
    /**
     * Prevent instantiation from outside should use MetastoreEventHandlerFactory instead
     */
    private DropDatabaseEventHandler(CatalogServiceCatalog catalog) { super(catalog); }

    /**
     * Process the drop database event. Currently, this handler removes the db object from
     * catalog. TODO Once we have HIVE-21077 we should compare creationTime to make sure
     * that catalog's Db matches with the database object in the event
     */
    @Override
    public void process(NotificationEvent event) {
      Pair<String, String> dbAndTblName =
          validateAndInit(DROP_DATABASE_EVENT_TYPE, event);
      // TODO this does not currently handle the case where the was a new instance
      // of database with the same name created in catalog after this database instance
      // was removed. For instance, user does a CREATE db, drop db and create db again
      // with the same dbName. In this case, the drop database event will remove the
      // database instance which is created after this create event. We should add a
      // check to compare the creation time of the database with the creation time in
      // the event to make sure we are removing the right databases object. Unfortunately,
      // database do not have creation time currently. This would be fixed in HIVE-21077
      Db removedDb = catalog_.removeDb(dbAndTblName.first);
      // if database did not exist in the cache there was nothing to do
      if (removedDb != null) {
        LOG.info(String.format("Successfully removed database %s", dbAndTblName.first));
      }
    }
  }

  /**
   * Event handler which issues invalidate on a table from the event
   */
  private static class TableInvalidatingEventHandler extends MetastoreEventHandler {
    /**
     * Prevent instantiation from outside should use MetastoreEventHandlerFactory instead
     */
    private TableInvalidatingEventHandler(CatalogServiceCatalog catalog) {
      super(catalog);
    }

    /**
     * Issues a invalidate table on the catalog on the table from the event. This
     * invalidate does not fetch information from metastore unlike the invalidate metadata
     * command since event is triggered post-metastore activity. This handler invalidates
     * by atomically removing existing loaded table and replacing it with a
     * IncompleteTable. If the table doesn't exist in catalog this operation is a no-op
     */
    @Override
    public void process(NotificationEvent event) {
      String dbName = Preconditions.checkNotNull(event.getDbName());
      String tblName = Preconditions.checkNotNull(event.getTableName());
      if (invalidateCatalogTable(dbName, tblName)) {
        LOG.info(String.format("Table %s.%s is invalidated after processing event "
                + "id %d of the type %s",
            dbName, tblName, event.getEventId(), event.getEventType()));
      } else {
        LOG.debug(String.format("Table %s.%s does not need to be "
                + "invalidated since "
                + "it does not exist anymore",
            dbName, tblName));
      }
    }
  }

  /**
   * Ignoring event handler that is used as a default handler for unknown/unsupported
   * event types
   */
  private static class IgnoringEventHandler extends MetastoreEventHandler {
    /**
     * Prevent instantiation from outside should use MetastoreEventHandlerFactory instead
     */
    private IgnoringEventHandler(CatalogServiceCatalog catalog) { super(catalog); }

    @Override
    public void process(NotificationEvent event) {
      String tblName = event.getTableName() != null ?
          event.getDbName() + "." + event.getTableName() :
          event.getDbName();
      LOG.debug(String.format("Ignoring event %d of type %s on object %s",
          event.getEventId(), event.getEventType(), tblName));
    }
  }
}
