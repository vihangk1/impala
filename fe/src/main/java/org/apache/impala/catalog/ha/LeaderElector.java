package org.apache.impala.catalog.ha;

import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.impala.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderElector implements Callable<Void> {

  final long leaseDuration;
  final long updateFrequency;
  long pingsSinceLastChange;
  final Path leaderDirectory;
  final int id;
  private long lastCheckedModificationTime = -1;
  private int lastLeaderFileId = 0;
  private static final Configuration CONF = new Configuration();
  private final FileSystem fs;
  private final AtomicBoolean isLeader = new AtomicBoolean(false);
  private long leaderFailureInterval;
  private int processingDelayBound;
  private static Random rand = new Random(System.currentTimeMillis());
  private static final Logger LOG = LoggerFactory.getLogger(LeaderElector.class);

  private final String leaderFileNameFormat = "catalog_leader_file_%d";

  public LeaderElector(int id, long leaseDuration, long updateFrequency, Path leaderDir)
      throws IOException {
    this.id = id;
    this.leaseDuration = leaseDuration;
    this.updateFrequency = updateFrequency;
    this.leaderDirectory = leaderDir;
    //TODO add supported file-systems check here
    fs = this.leaderDirectory.getFileSystem(CONF);
    // random fault injections for leaders
    leaderFailureInterval = rand.nextInt((int)updateFrequency*2) + 1;
    processingDelayBound = (int) updateFrequency * 2;
  }

  public boolean isLeader() { return isLeader.get(); }

  public static int getFileId(Path file) {
    String p = file.toString();
    int fileCount =
        Integer.parseInt(p.substring(p.lastIndexOf('_') + 1));
    return fileCount;
  }

  private Pair<Long, Integer> getLatestLeaderFile() throws LeaderElectorException {
    try {
      if (!fs.exists(leaderDirectory)) {
        fs.mkdirs(leaderDirectory);
      }
      FileStatus[] statuses = fs.listStatus(leaderDirectory);
      int count = 0;
      long modificationTime = -1;
      for (FileStatus status : statuses) {
        if (status.isFile()) {
          int fileCount = getFileId(status.getPath());
          if (fileCount > count) {
            count = fileCount;
            modificationTime = status.getModificationTime();
          }
        }
      }
      return new Pair<>(modificationTime, count);
    } catch (FileNotFoundException ex) {
      return new Pair(-1, 0);
    } catch (IOException ex) {
      ex.printStackTrace();
      throw new LeaderElectorException("Could not get modification time for leader "
          + "file", ex);
    }
  }

  private boolean updateLeaderFile(int fileid) throws LeaderElectorException {
    try {
      // this must be atomic rename. If the leader file already exists, return false
      // we try to rename this file to the next id. If there is already a nextId then
      // lease was renewed by some other catalog process
      // TODO can we replace this by createFile? Implement a reaper thread which
      //  cleans up old leader files which say the first 5
      //TODO figure out a way to add leader info in this file
      isLeader.set(fs.createNewFile(new Path(leaderDirectory,
          String.format(leaderFileNameFormat, fileid))));
      if (isLeader.get()) {
        lastLeaderFileId = fileid;
        System.out.println(System.currentTimeMillis() + " Thread " + id + " "
            + (isLeader.get() ? "Updated " : "Created") + " leader file");
      } else {
        System.out.println(System.currentTimeMillis() + " Thread " + id + " "
            + "could not create leader file");
      }
      return ;
    } catch (IOException e) {
      throw new LeaderElectorException(e.getMessage(), e);
    }
  }

  private boolean leaseExpired() {
    return pingsSinceLastChange > leaseDuration;
  }

  private void reset() {
    synchronized (isLeader) {
      isLeader.set(false);
      lastCheckedModificationTime = -1;
      pingsSinceLastChange = 0;
    }
  }

  private boolean isDead = false;

  // returns true if the leader is dead, otherwise false
  // returns false if this is not the leader
  private boolean injectLeaderFailure() {
    if (!isLeader.get())
      return false;
    if (leaderFailureInterval == 0) {
      isDead = !isDead;
      // failureInterval is complete, toggle the dead switch and return
      leaderFailureInterval = rand.nextInt((int)updateFrequency*2) + 1;
      System.out.println("Injecting " + (isDead ? " failure " : " recovery ") + "for "
          + "thread " + id + "Next fault will be injected in " + leaderFailureInterval
          + " cycles");
      if (!isDead) {
        // this is a recovery reset the state
        reset();
      }
      return isDead;
    }
    leaderFailureInterval--;
    return isDead;
  }

  @Override
  public Void call() {
    while (true) {
      try {
        if (!injectLeaderFailure()) {
          Pair<Long, Integer> leaderFileStatus = getLatestLeaderFile();
          boolean fileUpdated =
              (leaderFileStatus.second != lastLeaderFileId)
                  || (leaderFileStatus.first != lastCheckedModificationTime);
          if (fileUpdated) {
            // file has been updated leader is alive, renew leaseExpiryCounter
            pingsSinceLastChange = isLeader.get() ? leaseDuration / 2 : 0;
            lastCheckedModificationTime = leaderFileStatus.first;
            lastLeaderFileId = leaderFileStatus.second;
          } else {
            pingsSinceLastChange++;
          }
          System.out.println(System.currentTimeMillis() + " Thread " + id + " woke up. "
              + "Leader=" + isLeader.get() + " currentModificationTime="
              + leaderFileStatus.first + " timeout=" + (leaseDuration
              - pingsSinceLastChange));
          if (leaderFileStatus.first == -1
              || (leaseExpired() && !fileUpdated)) {
            // either leader file is not found, or lease is expired
            // attempt to create a leader file
            int nextFileId = leaderFileStatus.second + 1;
            // simulate processing delays like GC pauses
            simulateProcessingDelay();
            updateLeaderFile(nextFileId);
          }
        }
        Thread.sleep(updateFrequency);
      } catch (InterruptedException ex) {
        //
      } catch (LeaderElectorException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private void simulateProcessingDelay() {
    // simulate random processing delays
    long delay = rand.nextInt(processingDelayBound);
    LOG.debug("Simulating a processing delay of {}", delay);
    try {
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      //
    }
  }
  public int getId() {
    return id;
  }

  private static class LeaderElectorException extends Exception {

    LeaderElectorException(String msg, Throwable t) {
      super(msg, t);
    }
  }
}
