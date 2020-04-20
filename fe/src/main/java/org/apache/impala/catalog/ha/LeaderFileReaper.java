package org.apache.impala.catalog.ha;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderFileReaper implements Runnable {

  private final Path leaderFileDir;
  private final int threshold;
  private final FileSystem fs;
  private final int minFilesBeforeKickingIn;
  private static final Configuration CONF = new Configuration();
  private static final Logger LOG = LoggerFactory.getLogger(LeaderFileReaper.class);

  public LeaderFileReaper(Path leaderFileDir, int threshold, int minFilesBeforeKickIn)
      throws IOException {
    //TODO add precondition checks for inputs
    this.leaderFileDir = leaderFileDir;
    this.threshold = threshold;
    this.minFilesBeforeKickingIn = minFilesBeforeKickIn;
    this.fs = leaderFileDir.getFileSystem(CONF);
  }

  @Override
  public void run() {
    while (true) {
      try {
        FileStatus[] files = fs.listStatus(leaderFileDir);
        SortedMap<Integer, Path> fileMap = new TreeMap<>();
        for (FileStatus file : files) {
          if (file.isFile()) {
            int fileId = LeaderElector.getFileId(file.getPath());
            fileMap.put(fileId, file.getPath());
          }
        }
        if (fileMap.size() > minFilesBeforeKickingIn) {
          for (Path file : fileMap.headMap(fileMap.firstKey() + threshold).values()) {
            fs.delete(file, false);
            LOG.debug("Deleted file {}", file.toString());
          }
        }
        Thread.sleep(10);
      } catch (InterruptedException e) {

      } catch (FileNotFoundException e) {

      } catch (IOException e) {

      }
    }
  }
}
