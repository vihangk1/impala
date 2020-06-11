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

package org.apache.impala.catalog;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;

import com.google.common.base.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a pool of RetryingMetaStoreClient connections. If the connection pool is empty
 * a new client is created and added to the pool. The idle pool can expand till a maximum
 * size of MAX_HMS_CONNECTION_POOL_SIZE, beyond which the connections are closed.
 *
 * This default implementation reads the Hive metastore configuration from the HiveConf
 * object passed in the c'tor. If you are looking for a temporary HMS instance created
 * from scratch for unit tests, refer to EmbeddedMetastoreClientPool class. It mocks an
 * actual HMS by creating a temporary Derby backend database on the fly. It should not
 * be used for production Catalog server instances.
 */
public class MetaStoreClientPool {
  // Key for config option read from hive-site.xml
  private static final String HIVE_METASTORE_CNXN_DELAY_MS_CONF =
      "impala.catalog.metastore.cnxn.creation.delay.ms";
  private static final int DEFAULT_HIVE_METASTORE_CNXN_DELAY_MS_CONF = 0;
  // Maximum number of idle metastore connections in the connection pool at any point.
  private static final int MAX_HMS_CONNECTION_POOL_SIZE = 32;
  // Number of milliseconds to sleep between creation of HMS connections. Used to debug
  // IMPALA-825.
  private final int clientCreationDelayMs_;
  // singleton instance of this MetastoreClientPool
  private static MetaStoreClientPool pool_;

  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreClientPool.class);

  private final ConcurrentLinkedQueue<MetaStoreClient> clientPool_ =
      new ConcurrentLinkedQueue<MetaStoreClient>();
  private Boolean poolClosed_ = false;
  private final Object poolCloseLock_ = new Object();
  private final HiveConf hiveConf_;

  // Required for creating an instance of RetryingMetaStoreClient.
  private static final HiveMetaHookLoader dummyHookLoader = new HiveMetaHookLoader() {
    @Override
    public HiveMetaHook getHook(org.apache.hadoop.hive.metastore.api.Table tbl)
        throws MetaException {
      return null;
    }
  };

  /**
   * A wrapper around the RetryingMetaStoreClient that manages interactions with the
   * connection pool. This implements the AutoCloseable interface and hence the callers
   * should use the try-with-resources statement while creating an instance.
   */
  public class MetaStoreClient implements AutoCloseable {
    private final IMetaStoreClient hiveClient_;
    private boolean isInUse_;

    /**
     * Creates a new instance of MetaStoreClient.
     * 'cnxnTimeoutSec' specifies the time MetaStoreClient will wait to establish first
     * connection to the HMS before giving up and failing out with an exception.
     */
    private MetaStoreClient(HiveConf hiveConf, int cnxnTimeoutSec) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Creating MetaStoreClient. Pool Size = " + clientPool_.size());
      }

      long retryDelaySeconds = hiveConf.getTimeVar(
          HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);
      long retryDelayMillis = retryDelaySeconds * 1000;
      long endTimeMillis = System.currentTimeMillis() + cnxnTimeoutSec * 1000;
      IMetaStoreClient hiveClient = null;
      while (true) {
        try {
          hiveClient = RetryingMetaStoreClient.getProxy(hiveConf, dummyHookLoader,
              HiveMetaStoreClient.class.getName());
          break;
        } catch (Exception e) {
          // If time is up, throw an unchecked exception
          long delayUntilMillis = System.currentTimeMillis() + retryDelayMillis;
          if (delayUntilMillis >= endTimeMillis) throw new IllegalStateException(e);

          LOG.warn("Failed to connect to Hive MetaStore. Retrying.", e);
          while (delayUntilMillis > System.currentTimeMillis()) {
            try {
              Thread.sleep(delayUntilMillis - System.currentTimeMillis());
            } catch (InterruptedException | IllegalArgumentException ignore) {}
          }
        }
      }
      hiveClient_ = hiveClient;
      isInUse_ = false;
    }

    /**
     * Returns the internal RetryingMetaStoreClient object.
     */
    public IMetaStoreClient getHiveClient() {
      return hiveClient_;
    }

    /**
     * Returns this client back to the connection pool. If the connection pool has been
     * closed, just close the Hive client connection.
     */
    @Override
    public void close() {
      Preconditions.checkState(isInUse_);
      isInUse_ = false;
      // Ensure the connection isn't returned to the pool if the pool has been closed
      // or if the number of connections in the pool exceeds MAX_HMS_CONNECTION_POOL_SIZE.
      // This lock is needed to ensure proper behavior when a thread reads poolClosed
      // is false, but a call to pool.close() comes in immediately afterward.
      synchronized (poolCloseLock_) {
        if (poolClosed_ || clientPool_.size() >= MAX_HMS_CONNECTION_POOL_SIZE) {
          hiveClient_.close();
        } else {
          clientPool_.offer(this);
        }
      }
    }

    // Marks this client as in use
    private void markInUse() {
      Preconditions.checkState(!isInUse_);
      isInUse_ = true;
    }
  }

  /**
   * Returns the instance of MetastoreClientPool if it already exists. Else, creates one
   * based on configuration and returns it.
   */
  public static synchronized MetaStoreClientPool get() {
    if (pool_ != null) return pool_;

    if (MetastoreShim.getMajorVersion() > 2) {
      MetastoreShim.setHiveClientCapabilities();
    }
    pool_ = getMetastoreClientPool();
    return pool_;
  }

  /**
   * Creates appropriate pool based on the environment.
   */
  private static MetaStoreClientPool getMetastoreClientPool() {
    if (isTestMode()) {
      if (useEmbeddedClientPool()) return getEmbeddedPool();
      return getPoolForTests();
    }
    Preconditions
        .checkNotNull(BackendConfig.INSTANCE, "Backend configuration is not"
            + " initialized");
    TBackendGflags cfg = BackendConfig.INSTANCE.getBackendCfg();
    if (cfg.is_catalog) return getPoolForCatalog();
    return getPoolForCoordinator();
  }

  /**
   * Certain fe units tests instantiate Catalog service without backend services. They
   * must set the system property {@code catalog.in.test} to true.
   */
  private static boolean isTestMode() {
    return Boolean.parseBoolean(System.getProperty("catalog.in.test", "false"));
  }

  /**
   * Some fe unit tests instantiate a embedded client pool. They must set the system
   * property {@code catalog.test.mode} to embedded.
   * @return
   */
  private static boolean useEmbeddedClientPool() {
    Preconditions.checkState(isTestMode());
    return "embedded".equals(System.getProperty("catalog.test.mode", ""));
  }

  /**
   * Creates a Embedded metastore client pool. Currently used for certain tests.
   */
  private static MetaStoreClientPool getEmbeddedPool() {
    LOG.info(
        "Initializing embedded metastore client connection pool of size 1"
            + " and timeout 0 seconds.");
    Path derbyPath = Paths.get(System.getProperty("java.io.tmpdir"),
        UUID.randomUUID().toString());
    return new EmbeddedMetastoreClientPool(0, derbyPath);
  }

  /**
   * Creates a Metastore Client pool for Catalog service. It uses a pool size based on
   * backend configuration property {@code catalog_initial_hms_connections}
   */
  private static MetaStoreClientPool getPoolForCatalog() {
    TBackendGflags cfg = BackendConfig.INSTANCE.getBackendCfg();
    final int size = cfg.catalog_initial_hms_connections;
    Preconditions.checkState(size >= 0);
    LOG.info(
        "Initializing metastore client connection pool for catalog of size {}"
            + " and timeout {} seconds.",
        size, cfg.initial_hms_cnxn_timeout_s);
    return new MetaStoreClientPool(size,
        cfg.initial_hms_cnxn_timeout_s, new HiveConf(MetaStoreClientPool.class));
  }

  /**
   * Creates a Metastore client pool to be used in Coordinators. It uses a different pool
   * size than when instantiated for Catalog service which can be configured using the
   * backend configuration property {@code coordinator_initial_hms_connections}.
   * @return
   */
  private static MetaStoreClientPool getPoolForCoordinator() {
    Preconditions
        .checkNotNull(BackendConfig.INSTANCE, "Backend configuration is not"
            + " initialized");
    int poolSize = BackendConfig.INSTANCE
        .getBackendCfg().coordinator_initial_hms_connections;
    Preconditions.checkState(poolSize >= 0);
    LOG.info(
        "Initializing metastore client connection pool for coordinator of size {} "
            + "and timeout 0 seconds.", poolSize);
    //TODO (Vihang) why do we need a timeout of 0?
    return new MetaStoreClientPool(poolSize, 0,
        new HiveConf(MetaStoreClientPool.class));
  }

  /**
   * Creates a new MetastoreClientPool for tests. The default initial number of clients
   * in the pool is 1 (additional clients are created on-demand). To be used for
   * tests only.
   */
  private static MetaStoreClientPool getPoolForTests() {
    LOG.info(
        "Initializing metastore client connection pool for tests of size 1 "
            + "and timeout 0 seconds.");
    return new MetaStoreClientPool(1, 0, new HiveConf(MetaStoreClientPool.class));
  }

  @VisibleForTesting
  protected MetaStoreClientPool(int initialSize, int initialCnxnTimeoutSec,
      HiveConf hiveConf) {
    hiveConf_ = hiveConf;
    clientCreationDelayMs_ = hiveConf_.getInt(HIVE_METASTORE_CNXN_DELAY_MS_CONF,
        DEFAULT_HIVE_METASTORE_CNXN_DELAY_MS_CONF);
    initClients(initialSize, initialCnxnTimeoutSec);
  }

  /**
   * Initialize client pool with 'numClients' client.
   * 'initialCnxnTimeoutSec' specifies the time (in seconds) the first client will wait to
   * establish an initial connection to the HMS.
   */
  public void initClients(int numClients, int initialCnxnTimeoutSec) {
    Preconditions.checkState(clientPool_.size() == 0);
    if (numClients > 0) {
      clientPool_.add(new MetaStoreClient(hiveConf_, initialCnxnTimeoutSec));
      for (int i = 0; i < numClients - 1; ++i) {
        clientPool_.add(new MetaStoreClient(hiveConf_, 0));
      }
    }
  }

  /**
   * Gets a client from the pool. If the pool is empty a new client is created.
   */
  public MetaStoreClient getClient() {
    // The MetaStoreClient c'tor relies on knowing the Hadoop version by asking
    // org.apache.hadoop.util.VersionInfo. The VersionInfo class relies on opening
    // the 'common-version-info.properties' file as a resource from hadoop-common*.jar
    // using the Thread's context classloader. If necessary, set the Thread's context
    // classloader, otherwise VersionInfo will fail in it's c'tor.
    if (Thread.currentThread().getContextClassLoader() == null) {
      Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
    }

    MetaStoreClient client = clientPool_.poll();
    // The pool was empty so create a new client and return that.
    // Serialize client creation to defend against possible race conditions accessing
    // local Kerberos state (see IMPALA-825).
    if (client == null) {
      synchronized (this) {
        try {
          Thread.sleep(clientCreationDelayMs_);
        } catch (InterruptedException e) {
          /* ignore */
        }
        client = new MetaStoreClient(hiveConf_, 0);
      }
    }
    client.markInUse();
    return client;
  }

  /**
   * Removes all items from the connection pool and closes all Hive Meta Store client
   * connections. Can be called multiple times.
   */
  public void close() {
    // Ensure no more items get added to the pool once close is called.
    synchronized (poolCloseLock_) {
      if (poolClosed_) { return; }
      poolClosed_ = true;
    }

    MetaStoreClient client = null;
    while ((client = clientPool_.poll()) != null) {
      client.getHiveClient().close();
    }
    // if this pool is an instance of EmbeddedMetastoreClientPool
    // closeing the metastore client pool also means shutting down
    // the embedded metastore service. We should unset the pool_ variable
    // here so that next invocation of get() reinitializes a new embedded
    // metastore.
    if (pool_ instanceof EmbeddedMetastoreClientPool) {
      synchronized (MetaStoreClientPool.class) {
        LOG.info("Resetting the metastore client pool.");
        pool_ = null;
      }
    }
  }
}
