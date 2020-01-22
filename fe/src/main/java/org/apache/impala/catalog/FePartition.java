package org.apache.impala.catalog;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.impala.analysis.TableName;
import org.apache.impala.thrift.TCatalogObjectType;

public interface FePartition {
    /**
     * @return the ID for this partition which identifies it within its parent table.
     */
    long getId();
    /*
    boolean isLoaded();
    Partition getMetastorePartition();
    TCatalogObjectType getCatalogObjectType();
    TableName getTableName(); */
}