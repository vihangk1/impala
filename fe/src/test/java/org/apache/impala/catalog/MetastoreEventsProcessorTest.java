package org.apache.impala.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MetastoreEventsProcessorTest {

  private static final String TEST_TABLE_NAME_PARTITIONED = "test_partitioned_tbl";
  private static final String TEST_DB_NAME = "events_test_db";
  private static final String TEST_TABLE_NAME_NONPARTITIONED = "test_nonpartitioned_tbl";

  private static CatalogServiceCatalog catalog;
  private static MetastoreEventsProcessor eventsProcessor;
  private long beforeTestNotificationEventId;

  @BeforeClass
  public static void setUpTestClass() throws TException {
    catalog = CatalogServiceTestCatalog.create();
    eventsProcessor = catalog.getMetastoreEventProcessor();
    eventsProcessor.disableSchedulingForTests();
  }

  @AfterClass
  public static void tearDownTestSetup() {
    try {
      dropDatabaseCascade();
    } catch (Exception ex) {
      // ignored
    }
  }

  private static void dropDatabaseCascade() throws TException {
    dropDatabaseCascade(TEST_DB_NAME);
  }

  private static void dropDatabaseCascade(String dbName) throws TException {
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      msClient.getHiveClient().dropDatabase(dbName, true, true, true);
    }
  }

  @Before
  public void beforeTest() throws TException {
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      msClient.getHiveClient().dropDatabase(TEST_DB_NAME, true, true, true);
      CurrentNotificationEventId currentNotificationEventId = msClient.getHiveClient()
          .getCurrentNotificationEventId();
      beforeTestNotificationEventId = currentNotificationEventId.getEventId();
    }
  }

  @Test
  public void testCreateDatabaseEvent() throws TException, ImpalaException {
    createDatabase();
    eventsProcessor.processHMSNotificationEvents();
    assertNotNull(catalog.getDb(TEST_DB_NAME));
  }

  @Test
  public void testCreateTableEvent() throws TException, ImpalaException {
    createDatabase();
    // create a non-partitioned table
    createTable(TEST_TABLE_NAME_NONPARTITIONED, false);
    eventsProcessor.processHMSNotificationEvents();
    assertNotNull(
        "Catalog should have a incomplete instance of table after CREATE_TABLE event is received",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED));
    assertTrue("Newly created table from events should be a IncompleteTable",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED) instanceof IncompleteTable);
    // test partitioned table case
    createTable(TEST_TABLE_NAME_PARTITIONED, true);
    eventsProcessor.processHMSNotificationEvents();
    assertNotNull(
        "Catalog should have create a incomplete table after receiving CREATE_TABLE event",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED));
    assertTrue("Newly created table should be instance of IncompleteTable",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED) instanceof IncompleteTable);
  }

  @Test
  public void testPartitionEvent() throws TException, ImpalaException {
    createDatabase();
    createTable(TEST_TABLE_NAME_PARTITIONED, true);
    // sync to latest event id
    eventsProcessor.processHMSNotificationEvents();

    // simulate the table being loaded by explicitly calling load table
    loadTable(TEST_TABLE_NAME_PARTITIONED);
    List<List<String>> partVals = new ArrayList<>(1);

    // create 4 partitions
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    partVals.add(Arrays.asList("3"));
    partVals.add(Arrays.asList("4"));
    addPartitions(TEST_TABLE_NAME_PARTITIONED, partVals);

    eventsProcessor.processHMSNotificationEvents();
    // after ADD_PARTITION event is received currently we just invalidate the table
    assertTrue("Table should have been invalidated after add partition event",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED) instanceof IncompleteTable);

    loadTable(TEST_TABLE_NAME_PARTITIONED);
    assertEquals("Unexpected number of partitions fetched for the loaded table", 4,
        ((HdfsTable) catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED)).getPartitions()
            .size());

    // remove some partitions
    // change some partitions
  }

  @Test
  public void testAlterTableEvent() throws TException, ImpalaException {
    createDatabase();
    createTable("old_name", false);
    // sync to latest events
    eventsProcessor.processHMSNotificationEvents();
    // simulate the table being loaded by explicitly calling load table
    loadTable("old_name");

    // test renaming a table from outside aka metastore client
    alterTableRename("old_name", TEST_TABLE_NAME_NONPARTITIONED);
    eventsProcessor.processHMSNotificationEvents();
    // table with the old name should not be present anymore
    assertNull("Old named table is still existing",
        catalog.getTable(TEST_DB_NAME, "old_name"));
    // table with the new name should be present in Incomplete state
    Table newTable = catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED);
    assertNotNull("Table with the new name is not found", newTable);
    assertTrue("Table with the new name should be incomplete", newTable instanceof IncompleteTable);

    // check invalidate after alter table add parameter
    // Hive does not seem to create the events when table parameters are updated
    loadTable(TEST_TABLE_NAME_NONPARTITIONED);
    alterTableAddParameter(TEST_TABLE_NAME_NONPARTITIONED, "somekey", "someval");
    eventsProcessor.processHMSNotificationEvents();
    assertTrue("Table should be incomplete after alter table add parameter",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED) instanceof IncompleteTable);

    // check invalidate after alter table add col
    loadTable(TEST_TABLE_NAME_NONPARTITIONED);
    alterTableAddCol(TEST_TABLE_NAME_NONPARTITIONED, "newCol", "int", "null");
    eventsProcessor.processHMSNotificationEvents();
    assertTrue("Table should have been invalidated after alter table add column",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED) instanceof IncompleteTable);

    // check invalidate after alter table change column type
    loadTable(TEST_TABLE_NAME_NONPARTITIONED);
    altertableChangeCol(TEST_TABLE_NAME_NONPARTITIONED, "newCol", "string", null);
    eventsProcessor.processHMSNotificationEvents();
    assertTrue("Table should have been invalidated after changing column type",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED) instanceof IncompleteTable);

    // check invalidate after alter table remove column type
    loadTable(TEST_TABLE_NAME_NONPARTITIONED);
    alterTableRemoveCol(TEST_TABLE_NAME_NONPARTITIONED, "newCol");
    eventsProcessor.processHMSNotificationEvents();
    assertTrue("Table should have been invalidated after removing a column",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED) instanceof IncompleteTable);
  }

  @Test
  public void testDropTableEvent() throws TException, ImpalaException {
    createDatabase();
    final String TBL_TO_BE_DROPPED = "tbl_to_be_dropped";
    createTable(TBL_TO_BE_DROPPED, false);
    eventsProcessor.processHMSNotificationEvents();
    loadTable(TBL_TO_BE_DROPPED);
    // issue drop table and make sure it doesn't exist after processing the events
    dropTable(TBL_TO_BE_DROPPED);
    eventsProcessor.processHMSNotificationEvents();
    assertTrue("Table should not be found after processing drop_table event",
        catalog.getTable(TEST_DB_NAME, TBL_TO_BE_DROPPED) == null);

    // test partitioned table drop
    createTable(TBL_TO_BE_DROPPED, true);

    eventsProcessor.processHMSNotificationEvents();
    loadTable(TBL_TO_BE_DROPPED);
    // create 2 partitions
    List<List<String>> partVals = new ArrayList<>(2);
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    addPartitions(TBL_TO_BE_DROPPED, partVals);
    dropTable(TBL_TO_BE_DROPPED);
    eventsProcessor.processHMSNotificationEvents();
    assertTrue("Partitioned table should not be found after processing drop_table event",
        catalog.getTable(TEST_DB_NAME, TBL_TO_BE_DROPPED) == null);
  }

  @Test
  public void testdropDatabaseEvent() throws TException, ImpalaException {
    createDatabase();
    final String TBL_TO_BE_DROPPED = "tbl_to_be_dropped";
    createTable(TBL_TO_BE_DROPPED, true);
    createTable("tbl_to_be_dropped_unpartitioned", false);
    // create 2 partitions
    List<List<String>> partVals = new ArrayList<>(2);
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    addPartitions(TBL_TO_BE_DROPPED, partVals);
    eventsProcessor.processHMSNotificationEvents();
    loadTable(TBL_TO_BE_DROPPED);
    // now drop the database with cascade option
    dropDatabaseCascade();
    eventsProcessor.processHMSNotificationEvents();
    assertTrue("Partitioned table should not be found after processing drop_table event",
        catalog.getTable(TEST_DB_NAME, TBL_TO_BE_DROPPED) == null);
    assertTrue("Dropped database should not be found after processing drop_database event",
        catalog.getDb(TEST_DB_NAME) == null);

    // create empty database
    createDatabase("database_to_be_dropped");
    eventsProcessor.processHMSNotificationEvents();
    assertTrue(catalog.getDb("database_to_be_dropped") != null);
    dropDatabaseCascade("database_to_be_dropped");
    eventsProcessor.processHMSNotificationEvents();
    assertTrue("Database should not be found after processing drop_database event",
        catalog.getDb("database_to_be_dropped") == null);
  }


  private void createDatabase() throws TException {
    createDatabase(TEST_DB_NAME);
  }

  private void createDatabase(String dbName) throws TException {
    Database database = new DatabaseBuilder()
        .setName(dbName)
        .setDescription("Notification test database")
        .addParam("dbparamkey", "dbparamValue")
        .setOwnerName("NotificationTestOwner")
        .setOwnerType(PrincipalType.USER)
        .build();
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      msClient.getHiveClient().createDatabase(database);
    }
  }

  private void createTable(String tblName,
      boolean isPartitioned)
      throws TException {
    TableBuilder tblBuilder = new TableBuilder()
        .setTableName(tblName)
        .setDbName(TEST_DB_NAME)
        .addTableParam("tblParamKey", "tblParamValue")
        .addCol("c1", "string", "c1 description")
        .addCol("c2", "string", "c2 description")
        .setSerdeLib(HdfsFileFormat.PARQUET.serializationLib())
        .setInputFormat(HdfsFileFormat.PARQUET.inputFormat())
        .setOutputFormat(HdfsFileFormat.PARQUET.outputFormat());
    if (isPartitioned) {
      tblBuilder.addPartCol("p1", "string", "partition p1 description");
    }

    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      msClient.getHiveClient().createTable(tblBuilder.build());
    }
  }


  private void dropTable(String tableName) throws TException {
    try (MetaStoreClient client = catalog.getMetaStoreClient()) {
      client.getHiveClient().dropTable(TEST_DB_NAME, tableName, true, false);
    }
  }

  private void alterTableRename(String tblName, String newTblName)
      throws TException {
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = msClient.getHiveClient()
          .getTable(TEST_DB_NAME, tblName);
      msTable.setTableName(newTblName);
      msClient.getHiveClient()
          .alter_table_with_environmentContext(TEST_DB_NAME, tblName, msTable, null);
    }
  }

  private void alterTableAddParameter(String tblName, String key, String val)
      throws TException {
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = msClient.getHiveClient()
          .getTable(TEST_DB_NAME, tblName);
      msTable.getParameters().put(key, val);
      msClient.getHiveClient()
          .alter_table_with_environmentContext(TEST_DB_NAME, tblName, msTable, null);
    }
  }

  private void alterTableAddCol(String tblName, String colName, String colType,
      String comment)
      throws TException {
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = msClient.getHiveClient()
          .getTable(TEST_DB_NAME, tblName);
      msTable.getSd().getCols().add(new FieldSchema(colName, colType, comment));
      msClient.getHiveClient()
          .alter_table_with_environmentContext(TEST_DB_NAME, tblName, msTable, null);
    }
  }


  private void altertableChangeCol(String tblName, String colName, String colType, String comment)
      throws TException {
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = msClient.getHiveClient()
          .getTable(TEST_DB_NAME, tblName);
      FieldSchema targetCol = null;
      for (FieldSchema col : msTable.getSd().getCols()) {
        if (col.getName().equalsIgnoreCase(colName)) {
          targetCol = col;
          break;
        }
      }
      assertNotNull("Column " + colName + " does not exist", targetCol);
      targetCol.setName(colName);
      targetCol.setType(colType);
      targetCol.setComment(comment);
      msClient.getHiveClient()
          .alter_table_with_environmentContext(TEST_DB_NAME, tblName, msTable, null);
    }
  }

  private void alterTableRemoveCol(String tblName, String colName)
      throws TException {
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = msClient.getHiveClient()
          .getTable(TEST_DB_NAME, tblName);
      FieldSchema targetCol = null;
      for (FieldSchema col : msTable.getSd().getCols()) {
        if (col.getName().equalsIgnoreCase(colName)) {
          targetCol = col;
          break;
        }
      }
      assertNotNull("Column " + colName + " does not exist", targetCol);
      msTable.getSd().getCols().remove(targetCol);
      msClient.getHiveClient()
          .alter_table_with_environmentContext(TEST_DB_NAME, tblName, msTable, null);
    }
  }

  private void addPartitions(String tblName,
      List<List<String>> partitionValues)
      throws TException {
    int i = 0;
    List<Partition> partitions = new ArrayList<>(partitionValues.size());
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = msClient.getHiveClient()
          .getTable(TEST_DB_NAME, tblName);
      for (List<String> partVals : partitionValues) {
        partitions.add(
            new PartitionBuilder()
                .fromTable(msTable)
                .setInputFormat(msTable.getSd().getInputFormat())
                .setSerdeLib(msTable.getSd().getSerdeInfo().getSerializationLib())
                .setOutputFormat(msTable.getSd().getOutputFormat())
                .setValues(partVals)
                .build());
      }
    }
    try (MetaStoreClient metaStoreClient = catalog.getMetaStoreClient()) {
      metaStoreClient.getHiveClient().add_partitions(partitions);
    }
  }

  private Table loadTable(String tblName) throws CatalogException {
    Table loadedTable = catalog.getOrLoadTable(TEST_DB_NAME, tblName);
    assertFalse("Table should have been loaded after getOrLoadTable call",
        loadedTable instanceof IncompleteTable);
    return loadedTable;
  }
}
