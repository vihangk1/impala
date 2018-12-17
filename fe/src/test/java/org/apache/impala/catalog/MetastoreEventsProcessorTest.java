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
      cleanup();
    } catch (Exception ex) {
      // ignored
    }
  }

  private static void cleanup() throws TException {
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      msClient.getHiveClient().dropDatabase(TEST_DB_NAME, true, true, true);
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

  private void createTable(String dbName, String tblName,
      boolean isPartitioned)
      throws TException {
    TableBuilder tblBuilder = new TableBuilder()
        .setTableName(tblName)
        .setDbName(dbName)
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

  private void alterTableRename(String dbName, String tblName, String newTblName)
      throws TException {
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = msClient.getHiveClient()
          .getTable(dbName, tblName);
      msTable.setTableName(newTblName);
      msClient.getHiveClient().alter_table_with_environmentContext(dbName, tblName, msTable, null);
    }
  }

  private void alterTableAddParameter(String dbName, String tblName, String key, String val)
      throws TException {
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = msClient.getHiveClient()
          .getTable(dbName, tblName);
      msTable.getParameters().put(key, val);
      msClient.getHiveClient().alter_table_with_environmentContext(dbName, tblName, msTable, null);
    }
  }

  private void alterTableAddCol(String dbName, String tblName, String colName, String colType,
      String comment)
      throws TException {
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = msClient.getHiveClient()
          .getTable(dbName, tblName);
      msTable.getSd().getCols().add(new FieldSchema(colName, colType, comment));
      msClient.getHiveClient().alter_table_with_environmentContext(dbName, tblName, msTable, null);
    }
  }

  private void alterTableRemoveCol(String dbName, String tblName, String colName)
      throws TException {
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = msClient.getHiveClient()
          .getTable(dbName, tblName);
      FieldSchema targetCol = null;
      for (FieldSchema col : msTable.getSd().getCols()) {
        if (col.getName().equalsIgnoreCase(colName)) {
          targetCol = col;
          break;
        }
      }
      assertNotNull("Column " + colName + " does not exist", targetCol);
      msTable.getSd().getCols().remove(targetCol);
      msClient.getHiveClient().alter_table_with_environmentContext(dbName, tblName, msTable, null);
    }
  }

  private void addPartitions(String dbName, String tblName,
      List<List<String>> partitionValues)
      throws TException {
    int i = 0;
    List<Partition> partitions = new ArrayList<>(partitionValues.size());
    try (MetaStoreClient msClient = catalog.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable = msClient.getHiveClient()
          .getTable(dbName, tblName);
      partitions.add(
          new PartitionBuilder()
              .fromTable(msTable)
              .setValues(partitionValues.get(i++))
              .build());
    }
    try (MetaStoreClient metaStoreClient = catalog.getMetaStoreClient()) {
      metaStoreClient.getHiveClient().add_partitions(partitions);
    }
  }


  @Test
  public void testCreateDatabase() throws TException, ImpalaException {
    createDatabase(TEST_DB_NAME);
    eventsProcessor.processHMSNotificationEvents();
    assertNotNull(catalog.getDb(TEST_DB_NAME));
  }

  @Test
  public void testCreateTable() throws TException, ImpalaException {
    createDatabase(TEST_DB_NAME);
    // create a non-partitioned table
    createTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED, false);
    eventsProcessor.processHMSNotificationEvents();
    assertNotNull(
        "Catalog should have a incomplete instance of table after CREATE_TABLE event is received",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED));
    assertTrue("Newly created table from events should be a IncompleteTable",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED) instanceof IncompleteTable);
    // test partitioned table case
    createTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED, true);
    eventsProcessor.processHMSNotificationEvents();
    assertNotNull(
        "Catalog should have create a incomplete table after receiving CREATE_TABLE event",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED));
    assertTrue("Newly created table should be instance of IncompleteTable",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED) instanceof IncompleteTable);
  }

  @Test
  public void testPartitionEvents() throws TException, ImpalaException {
    createDatabase(TEST_DB_NAME);
    createTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED, true);
    // sync to latest event id
    eventsProcessor.processHMSNotificationEvents();

    // simulate the table being loaded by explicitly calling load table
    Table loadedTable = catalog
        .getOrLoadTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED);
    assertFalse("Table should not be Incomplete after a load call is issued",
        loadedTable instanceof IncompleteTable);
    List<List<String>> partVals = new ArrayList<>(1);

    // create 4 partitions
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    partVals.add(Arrays.asList("3"));
    partVals.add(Arrays.asList("4"));
    addPartitions(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED, partVals);

    eventsProcessor.processHMSNotificationEvents();
    // after ADD_PARTITION event is received currently we just invalidate the table
    assertTrue("Table should have been invalidated after add partition event",
        catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED) instanceof IncompleteTable);

    loadedTable = catalog.getOrLoadTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED);
    assertFalse(loadedTable instanceof IncompleteTable);
    assertEquals("Unexpected number of partitions fetched for the loaded table", 4,
        ((HdfsTable) loadedTable).getPartitions().size());
  }

  @Test
  public void testAlterTable() throws TException, ImpalaException {
    createDatabase(TEST_DB_NAME);
    createTable(TEST_DB_NAME, "old_name", false);
    // sync to latest events
    eventsProcessor.processHMSNotificationEvents();
    // simulate the table being loaded by explicitly calling load table
    Table loadedTable = catalog
        .getOrLoadTable(TEST_DB_NAME, "old_name");
    assertFalse("Table should not be Incomplete after a load call is issued",
        loadedTable instanceof IncompleteTable);

    // issue alter table rename out of band
    alterTableRename(TEST_DB_NAME, "old_name", TEST_TABLE_NAME_NONPARTITIONED);
    eventsProcessor.processHMSNotificationEvents();
    // table with the old name should not be present anymore
    assertNull("Old named table is still existing",
        catalog.getTable(TEST_DB_NAME, "old_name"));
    // table with the new name should be present in Incomplete state
    Table newTable = catalog.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED);
    assertNotNull("Table with the new name is not found", newTable);
    assertTrue("Table with the new name should be incomplete", newTable instanceof IncompleteTable);

    catalog.getOrLoadTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED);
    // issue alter table add parameter
    alterTableAddParameter(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED, "somekey", "someval");
    eventsProcessor.processHMSNotificationEvents();
    // after alter_table table should have been invalidated
    assertTrue("Table with the new name should be incomplete", newTable instanceof IncompleteTable);
  }
}
