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

import org.apache.impala.authorization.NoopAuthorizationFactory.NoopAuthorizationManager;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TUniqueId;

/**
 * Creates a transient catalog which uses an embedded Metastore as the backing service.
 */
public class CatalogServiceTestTransientCatalog {
  static {
    System.setProperty("catalog.in.test", "true");
    System.setProperty("catalog.test.mode", "embedded");
  }

  /**
   * Creates a transient test catalog instance backed by an embedded HMS derby database on
   * the local filesystem. The derby database is created from scratch and has no table
   * metadata.
   */
  public static CatalogServiceCatalog create() throws
      ImpalaException {
    FeSupport.loadLibrary();
    CatalogServiceCatalog cs = new CatalogServiceTestCatalog(false, 16,
        new TUniqueId());
    cs.setAuthzManager(new NoopAuthorizationManager());
    cs.reset();
    return cs;
  }
}
