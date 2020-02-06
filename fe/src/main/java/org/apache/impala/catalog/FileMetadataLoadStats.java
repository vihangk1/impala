package org.apache.impala.catalog;

import com.google.common.base.Objects;
import org.apache.hadoop.fs.Path;

// File/Block metadata loading stats for a single HDFS path.
public class FileMetadataLoadStats {
  //TODO(Vihang) change these members to private
  public Path partDir_;

  /** Number of files skipped because they pertain to an uncommitted ACID transaction */
  public int uncommittedAcidFilesSkipped = 0;

  /**
   * Number of files skipped because they pertain to ACID directories superceded
   * by later base data.
   */
  public int filesSupercededByNewerBase = 0;

  // Number of files for which the metadata was loaded.
  public int loadedFiles = 0;

  // Number of hidden files excluded from file metadata loading. More details at
  // isValidDataFile().
  public int hiddenFiles = 0;

  // Number of files skipped from file metadata loading because the files have not
  // changed since the last load. More details at hasFileChanged().
  //
  // TODO(todd) rename this to something indicating it was fast-pathed, not skipped
  public int skippedFiles = 0;

  // Number of unknown disk IDs encountered while loading block
  // metadata for this path.
  public int unknownDiskIds = 0;

  public String debugString() {
    return Objects.toStringHelper("")
        .add("path", partDir_)
        .add("loaded files", loadedFiles)
        .add("hidden files", nullIfZero(hiddenFiles))
        .add("skipped files", nullIfZero(skippedFiles))
        .add("uncommited files", nullIfZero(uncommittedAcidFilesSkipped))
        .add("superceded files", nullIfZero(filesSupercededByNewerBase))
        .add("unknown diskIds", nullIfZero(unknownDiskIds))
        .omitNullValues()
        .toString();
  }

  private Integer nullIfZero(int x) {
    return x > 0 ? x : null;
  }

  public void addAll(FileMetadataLoadStats other) {
    this.uncommittedAcidFilesSkipped += other.uncommittedAcidFilesSkipped;
    this.filesSupercededByNewerBase += other.filesSupercededByNewerBase;
    this.loadedFiles += other.loadedFiles;
    this.hiddenFiles += other.hiddenFiles;
    this.skippedFiles += other.skippedFiles;
    this.unknownDiskIds += other.unknownDiskIds;
  }
}
