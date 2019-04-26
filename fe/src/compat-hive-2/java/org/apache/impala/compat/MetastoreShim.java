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

package org.apache.impala.compat;

import static org.apache.impala.service.MetadataOp.TABLE_TYPE_TABLE;
import static org.apache.impala.service.MetadataOp.TABLE_TYPE_VIEW;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.json.ExtendedJSONMessageFactory;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetFunctionsReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.impala.authorization.User;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.service.Frontend;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TResultSet;
import org.apache.thrift.TException;

/**
 * A wrapper around some of Hive's Metastore API's to abstract away differences
 * between major versions of Hive. This implements the shimmed methods for Hive 2.
 */
public class MetastoreShim {
  /**
   * Wrapper around MetaStoreUtils.validateName() to deal with added arguments.
   */
  public static boolean validateName(String name) {
    return MetaStoreUtils.validateName(name, null);
  }

  /**
   * Wrapper around IMetaStoreClient.alter_partition() to deal with added
   * arguments.
   */
  public static void alterPartition(IMetaStoreClient client, Partition partition)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partition(
        partition.getDbName(), partition.getTableName(), partition, null);
  }

  /**
   * Wrapper around IMetaStoreClient.alter_partitions() to deal with added
   * arguments.
   */
  public static void alterPartitions(IMetaStoreClient client, String dbName,
      String tableName, List<Partition> partitions)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partitions(dbName, tableName, partitions, null);
  }

  /**
   * Wrapper around MetaStoreUtils.updatePartitionStatsFast() to deal with added
   * arguments.
   */
  public static void updatePartitionStatsFast(Partition partition, Table tbl,
      Warehouse warehouse) throws MetaException {
    MetaStoreUtils.updatePartitionStatsFast(partition, warehouse, null);
  }

  /**
   * Return the maximum number of Metastore objects that should be retrieved in
   * a batch.
   */
  public static String metastoreBatchRetrieveObjectsMaxConfigKey() {
    return HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_OBJECTS_MAX.toString();
  }

  /**
   * Return the key and value that should be set in the partition parameters to
   * mark that the stats were generated automatically by a stats task.
   */
  public static Pair<String, String> statsGeneratedViaStatsTaskParam() {
    return Pair.create(StatsSetupConst.STATS_GENERATED, StatsSetupConst.TASK);
  }

  public static TResultSet execGetFunctions(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetFunctionsReq req = request.getGet_functions_req();
    return MetadataOp.getFunctions(
        frontend, req.getCatalogName(), req.getSchemaName(), req.getFunctionName(), user);
  }

  public static TResultSet execGetColumns(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetColumnsReq req = request.getGet_columns_req();
    return MetadataOp.getColumns(frontend, req.getCatalogName(), req.getSchemaName(),
        req.getTableName(), req.getColumnName(), user);
  }

  public static TResultSet execGetTables(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetTablesReq req = request.getGet_tables_req();
    return MetadataOp.getTables(frontend, req.getCatalogName(), req.getSchemaName(),
        req.getTableName(), req.getTableTypes(), user);
  }

  public static TResultSet execGetSchemas(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetSchemasReq req = request.getGet_schemas_req();
    return MetadataOp.getSchemas(
        frontend, req.getCatalogName(), req.getSchemaName(), user);
  }

  // TableTypes supported in HMS 2
  public static EnumSet<TableType> getTableTypes() {
    return EnumSet
        .of(TableType.EXTERNAL_TABLE, TableType.MANAGED_TABLE, TableType.VIRTUAL_VIEW);
  }

  /**
   * Method which maps Metastore's TableType to Impala's table type. In metastore 2
   * Materialized view is not supported
   */
  public static String mapToInternalTableType(String typeStr) {
    String defaultTableType = TABLE_TYPE_TABLE;
    TableType tType;

    if (typeStr == null) return defaultTableType;
    Preconditions.checkArgument(!"MATERIALIZED_VIEW".equals(typeStr.toUpperCase()),
        "Materialized view is not supported in the metastore version 2");
    try {
      tType = TableType.valueOf(typeStr.toUpperCase());
    } catch (Exception e) {
      return defaultTableType;
    }
    switch (tType) {
      case EXTERNAL_TABLE:
      case MANAGED_TABLE:
      case INDEX_TABLE:
        return TABLE_TYPE_TABLE;
      case VIRTUAL_VIEW:
        return TABLE_TYPE_VIEW;
      default:
        return defaultTableType;
    }
  }

  /**
   * Wrapper method which returns ExtendedJSONMessageFactory in case Impala is
   * building against Hive-2 to keep compatibility with Sentry
   */
  public static MessageDeserializer getMessageDeserializer() {
    return ExtendedJSONMessageFactory.getInstance().getDeserializer();
  }

  /**
   * Wrapper around FileUtils.makePartName to deal with package relocation in Hive 3
   * @param partitionColNames
   * @param values
   * @return
   */
  public static String makePartName(List<String> partitionColNames, List<String> values) {
    return FileUtils.makePartName(partitionColNames, values);
  }

  /**
   * Wrapper method around message factory's build alter table message due to added
   * arguments in hive 3.
   */
  @VisibleForTesting
  public static AlterTableMessage buildAlterTableMessage(Table before, Table after,
      boolean b, long l) {
    return ExtendedJSONMessageFactory.getInstance().buildAlterTableMessage(before, after);
  }
}
