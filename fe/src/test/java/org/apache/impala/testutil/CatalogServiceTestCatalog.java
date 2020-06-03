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

package org.apache.impala.testutil;

import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.authorization.NoopAuthorizationFactory.NoopAuthorizationManager;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TUniqueId;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Test class of the Catalog Server's catalog that exposes internal state that is useful
 * for testing.
 */
public class CatalogServiceTestCatalog extends CatalogServiceCatalog {
  static {
    // this property is used by MetastoreClientPool to instantiate a test client pool
    System.setProperty("catalog.in.test", "true");
  }

  public CatalogServiceTestCatalog(boolean loadInBackground, int numLoadingThreads,
      TUniqueId catalogServiceId)
      throws ImpalaException {
    super(loadInBackground, numLoadingThreads, catalogServiceId,
        System.getProperty("java.io.tmpdir"));

    // Cache pools are typically loaded asynchronously, but as there is no fixed execution
    // order for tests, the cache pools are loaded synchronously before the tests are
    // executed.
    CachePoolReader rd = new CachePoolReader(false);
    rd.run();
  }

  public static CatalogServiceCatalog create() {
    return createWithAuth(new NoopAuthorizationFactory());
  }

  /**
   * Creates a catalog server that reads authorization policy metadata from the
   * authorization config.
   */
  public static CatalogServiceCatalog createWithAuth(AuthorizationFactory authzFactory) {
    FeSupport.loadLibrary();
    CatalogServiceCatalog cs;
    try {
      cs = new CatalogServiceTestCatalog(false, 16, new TUniqueId());
      cs.setAuthzManager(authzFactory.newAuthorizationManager(cs));
      cs.reset();
    } catch (ImpalaException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
    return cs;
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() { return authPolicy_; }
}
