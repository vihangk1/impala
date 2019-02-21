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

package org.apache.impala.catalog.events;

import org.apache.impala.catalog.CatalogException;
import org.apache.impala.thrift.TGetEventProcessorMetricsResponse;

/**
 * Interface to process external events
 */
public interface ExternalEventsProcessor {
  /**
   * Start the event processing. This could also be used to initialize the configuration
   * like polling interval of the event processor
   */
  void start();

  /**
   * Get the current event id on metastore. Useful for restarting the event processing
   * from a given event id
   */
  long getCurrentEventId() throws CatalogException;

  /**
   * Stop the event processing
   */
  void stop();

  /**
   * Starts the event processing from the given eventId. This method can be used to jump
   * ahead in the event processing under certain cases where it is okay skip certain
   * events
   */
  void start(long fromEventId);

  /**
   * Implements the core logic of processing external events
   */
  void processEvents();

  TGetEventProcessorMetricsResponse getEventProcessorMetrics();
}