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

package org.apache.impala.common;

import static org.apache.impala.common.FileSystemUtil.HIVE_TEMP_FILE_PREFIX;
import static org.apache.impala.common.FileSystemUtil.isIgnoredDir;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for the various util methods in FileSystemUtil class
 */
public class FileSystemUtilTest {

  private static final Path TEST_TABLE_PATH = new Path("/test-warehouse/foo"
      + ".db/filesystem-util-test");

  @Test
  public void testLeaderElection() throws Exception {
    LeaderElector leaderElector1 = new LeaderElector(1, 30, 1000, "hdfs:///tmp"
        + "/catalog_leader");
    LeaderElector leaderElector2 = new LeaderElector(2, 30, 1000, "hdfs:///tmp"
        + "/catalog_leader");
    LeaderMonitor monitor = new LeaderMonitor(10, leaderElector1, leaderElector2);
    ExecutorService service = Executors.newFixedThreadPool(3);
    Future<Void> task1 = service.submit(leaderElector1);
    Future<Void> task2 = service.submit(leaderElector2);
    Future<Void> monitorTask = service.submit(monitor);
    service.shutdown();
    //monitorTask.get();
    task1.get();
    task2.get();
  }

  private static class LeaderElectorException extends Exception {

    LeaderElectorException(String msg, Throwable t) {
      super(msg, t);
    }
  }

  private static class LeaderElector implements Callable<Void> {

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
    private static Random rand = new Random(System.currentTimeMillis());

    private final String leaderFileNameFormat = "catalog_leader_file_%d";

    LeaderElector(int id, long leaseDuration, long updateFrequency, String leaderDir)
        throws IOException {
      this.id = id;
      this.leaseDuration = leaseDuration;
      this.updateFrequency = updateFrequency;
      this.leaderDirectory = new Path(leaderDir);
      fs = this.leaderDirectory.getFileSystem(CONF);
      // random fault injections for leaders
      leaderFailureInterval = rand.nextInt(60) + 1;
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
            String p = status.getPath().toString();
            String filename = p.substring(p.lastIndexOf('/') + 1);
            int fileCount =
                Integer.parseInt(filename.substring(filename.lastIndexOf('_') + 1));
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
      Path tmpFileName = new Path("hdfs:///tmp/__tmp__" + id);
      try (FSDataOutputStream out = fs
          .create(tmpFileName, true)) {
        out.writeBytes("leader=" + id);
      } catch (IOException ex) {
        throw new LeaderElectorException("Could not create temp file for leader + id",
            ex);
      }
      try {
        // this must be atomic rename. If the leader file already exists, return false
        // we try to rename this file to the next id. If there is already a nextId then
        // lease was renewed by some other catalog process
        // TODO can we replace this by createFile? Implement a reaper thread which
        //  cleans up old leader files which say the first 5
        boolean ret = fs.rename(tmpFileName, new Path(leaderDirectory,
            String.format(leaderFileNameFormat, fileid)));
        if (ret) {
          System.out.println(System.currentTimeMillis() + " Thread " + id + " "
              + (isLeader.get() ? "Updated " : "Created") + " leader file");
        } else {
          System.out.println(System.currentTimeMillis() + " Thread " + id + " "
              + "could not create leader file");
        }
        return ret;
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
        System.out.println("Injecting " + (isDead ? " failure " : " recovery ") + "for "
            + "thread " + id);
        leaderFailureInterval = rand.nextInt(60) + 1;
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
              boolean success = updateLeaderFile(nextFileId);
              if (success) {
                lastLeaderFileId = nextFileId;
                isLeader.set(true);
              } else {
                isLeader.set(false);
              }
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
  }

  @Test
  public void testIsInIgnoredDirectory() {
    // test positive cases
    assertTrue("Files in hive staging directory should be ignored",
        testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH, "/part=1/"
            + ".hive-staging/tempfile")));

    assertTrue("Files in hidden directory ignored",
        testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH, ".hidden/000000_0")));

    assertTrue("Files in the hive temporary directories should be ignored",
        testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH,
            HIVE_TEMP_FILE_PREFIX + "base_0000000_1/000000_1.manifest")));

    assertTrue("Files in hive temporary directories should be ignored",
        testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH,
            HIVE_TEMP_FILE_PREFIX + "delta_000000_2/test.manifest")));

    //multiple nested levels
    assertTrue(testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH,
        ".hive-staging/nested-1/nested-2/nested-3/tempfile")));

    // test negative cases
    // table path should not ignored
    assertFalse(testIsInIgnoredDirectory(TEST_TABLE_PATH));
    assertFalse(
        testIsInIgnoredDirectory(new Path("hdfs://localhost:20500" + TEST_TABLE_PATH)));
    // partition path
    assertFalse(testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH + "/part=1/000000")));
    assertFalse(testIsInIgnoredDirectory(
        new Path("hdfs://localhost:20500" + TEST_TABLE_PATH + "/part=1/00000")));
    // nested directories for ACID tables should not be ignored
    assertFalse(testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH, "/part=100"
        + "/base_0000005/datafile")));
    assertFalse(testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH,
        "/delta_0000001_0000002/deltafile")));

  }

  @Test
  public void testIsIgnoredDir() {
    assertTrue("Directory should be ignored if it starts with _tmp.",
        isIgnoredDir(new Path(TEST_TABLE_PATH, HIVE_TEMP_FILE_PREFIX + "dummy")));
    assertTrue("Directory should be ignored if its hidden",
        isIgnoredDir(new Path(TEST_TABLE_PATH, ".hidden-dir")));
    assertFalse(isIgnoredDir(TEST_TABLE_PATH));
    assertFalse(isIgnoredDir(new Path(TEST_TABLE_PATH + "/part=100/datafile")));
  }

  private boolean testIsInIgnoredDirectory(Path input) {
    return testIsInIgnoredDirectory(input, true);
  }

  private boolean testIsInIgnoredDirectory(Path input, boolean isDir) {
    FileStatus mockFileStatus = Mockito.mock(FileStatus.class);
    Mockito.when(mockFileStatus.getPath()).thenReturn(input);
    Mockito.when(mockFileStatus.isDirectory()).thenReturn(isDir);
    return FileSystemUtil.isInIgnoredDirectory(TEST_TABLE_PATH, mockFileStatus);
  }

  private static class LeaderMonitor implements Callable<Void> {
    final long frequency;
    final List<LeaderElector> electors;
    LeaderMonitor(long frequency, LeaderElector... electors) {
      this.frequency = frequency;
      this.electors = ImmutableList.copyOf(electors);
    }

    @Override
    public Void call() throws Exception {
      while(true) {
        boolean leaderFound = false;
        int leaderId = -1;
        for (LeaderElector elector : electors) {
          boolean isLeader = elector.isLeader.get();
          if (isLeader) {
            if (leaderFound) {
              System.out.println(String.format("ERROR: Multiple leaders found %d and %d",
                  elector.id, leaderId));
            }
            leaderId = elector.id;
            leaderFound = true;
          }
        }
        Thread.sleep(frequency);
      }
    }
  }
}
