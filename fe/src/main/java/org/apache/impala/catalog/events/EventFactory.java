package org.apache.impala.catalog.events;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;

public interface EventFactory {
  MetastoreEvent get(NotificationEvent hmsEvent) throws MetastoreNotificationException;
}
