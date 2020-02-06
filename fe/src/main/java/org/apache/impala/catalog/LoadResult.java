package org.apache.impala.catalog;


import com.google.common.base.Preconditions;
import java.util.List;
import com.google.common.collect.Lists;

public class LoadResult {

  public static final LoadResult EMPTY_RESULT = new LoadResult();

  private FileMetadataLoadStats fileMetadataLoadStats_;
  private List<HdfsPartition> partitionsUpdated_ = Lists.newArrayList();
  private List<HdfsPartition> partitionsRemoved_ = Lists.newArrayList();

  public void setFileMetadataLoadStats_(
      FileMetadataLoadStats fileMetadataLoadStats_) {
    this.fileMetadataLoadStats_ = fileMetadataLoadStats_;
  }

  public FileMetadataLoadStats getFileMetadataLoadStats() {
    return fileMetadataLoadStats_;
  }

  public void addToPartitionsUpdated(HdfsPartition part) {
    Preconditions.checkNotNull(part);
    partitionsUpdated_.add(part);
  }

  public void addToPartitionsRemoved(HdfsPartition part) {
    Preconditions.checkNotNull(part);
    partitionsRemoved_.add(part);
  }

  public List<HdfsPartition> getPartitionsUpdated_() {
    return partitionsUpdated_;
  }

  public List<HdfsPartition> getPartitionsRemoved_() {
    return partitionsRemoved_;
  }

  public void addToPartitionsUpdated(
      List<HdfsPartition> partitions) {
    Preconditions.checkNotNull(partitions);
    partitionsUpdated_.addAll(partitions);
  }

  public void addToPartitionsRemoved(List<HdfsPartition> dropPartitions) {
    Preconditions.checkNotNull(dropPartitions);
    partitionsRemoved_.addAll(dropPartitions);
  }
}
