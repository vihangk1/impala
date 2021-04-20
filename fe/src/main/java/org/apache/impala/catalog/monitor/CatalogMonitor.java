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

package org.apache.impala.catalog.monitor;

import org.apache.impala.common.Metrics;

/**
 * Singleton class that helps monitor catalog, monitoring classes can be
 * accessed through the CatalogMonitor.
 */
public final class CatalogMonitor {
  public final static CatalogMonitor INSTANCE = new CatalogMonitor();

  private final CatalogTableMetrics catalogTableMetrics_;

  private final CatalogOperationMetrics catalogOperationUsage_;

  private final Metrics catalogdHmsCacheMetrics_ = new Metrics();

  private CatalogMonitor() {
    catalogTableMetrics_ = CatalogTableMetrics.INSTANCE;
    catalogOperationUsage_ = CatalogOperationMetrics.INSTANCE;
  }

  public CatalogTableMetrics getCatalogTableMetrics() { return catalogTableMetrics_; }

  public CatalogOperationMetrics getCatalogOperationMetrics() {
    return catalogOperationUsage_;
  }

  public Metrics getCatalogdHmsCacheMetrics() {
    return catalogdHmsCacheMetrics_;
  }
}
