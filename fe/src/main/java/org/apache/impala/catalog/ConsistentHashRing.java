package org.apache.impala.catalog;

import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashRing {
  public static final ConsistentHashRing INSTANCE = new ConsistentHashRing();
  private final SortedMap<Integer, String> ring = new TreeMap<>();
  //TODO increase this value
  private final int NUM_OF_VNODES = 1;

  public synchronized void updateNode(String serviceId, boolean removed) {
    if (removed) {
      removeNode(serviceId);
      return;
    }
    addNode(serviceId);
  }

  private void addNode(String serviceId) {
    for (int i=0; i<NUM_OF_VNODES; i++) {
      ring.put(hash(serviceId + i), serviceId);
    }
  }

  private Integer hash(String s) {
    //TODO use a better hash function (may be MD5)
    return s.hashCode() % Integer.MAX_VALUE;
  }

  private void removeNode(String serviceId) {
    for (int i=0; i<NUM_OF_VNODES; i++) {
      ring.remove(hash(serviceId + i));
    }
  }

  public synchronized String getServiceId(String objectKey) {
    Integer objHash = hash(objectKey);
    if (ring.containsKey(objHash)) {
      return ring.get(objHash);
    }
    SortedMap<Integer, String> tailMap = ring.tailMap(objHash);
    Integer nearestNodeId = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
    return ring.get(nearestNodeId);
  }
}
