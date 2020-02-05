package org.apache.impala.catalog;

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;

public class PartitionLoadArgs {
  private final boolean recursive;
  private final ValidTxnList validTxnList;
  private final ValidWriteIdList validWriteIdList;

  public PartitionLoadArgs(boolean recursive,
      ValidTxnList validTxnList,
      ValidWriteIdList validWriteIdList) {
    this.recursive = recursive;
    this.validTxnList = validTxnList;
    this.validWriteIdList = validWriteIdList;
  }

  public boolean isRecursive() {
    return recursive;
  }

  public ValidTxnList getValidTxnList() {
    return validTxnList;
  }

  public ValidWriteIdList getValidWriteIdList() {
    return validWriteIdList;
  }
}
