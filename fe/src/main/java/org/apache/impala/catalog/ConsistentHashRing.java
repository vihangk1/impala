package org.apache.impala.catalog;

import com.google.common.base.Preconditions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsistentHashRing {
  public static final ConsistentHashRing INSTANCE = new ConsistentHashRing();
  private final SortedMap<Long, String> ring = new TreeMap<>();
  //TODO increase this value
  private final int NUM_OF_VNODES = 1;
  private static final Logger LOG = LoggerFactory.getLogger(ConsistentHashRing.class);
  private static final int PRINT_INTERVAL = 10;
  private int currentCount = 0;
  private final MessageDigest messageDigest_;

  private ConsistentHashRing() {
    try {
      messageDigest_ = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Unable to instantiated message digest", e);
      throw new RuntimeException(e);
    }
  }

  public synchronized void updateNode(String serviceId, boolean removed) {
    Preconditions.checkNotNull(serviceId);
    LOG.trace("Received a update Node request: service id {} removed: {}", serviceId,
        removed);
    if (removed) {
      removeNode(serviceId);
      return;
    }
    addNode(serviceId);
    if ((currentCount++ % PRINT_INTERVAL) == 0) {
      LOG.info(INSTANCE.toString());
    }
  }

  @Override
  public String toString() {
    return StringUtils.join(ring);
  }

  private void addNode(String serviceId) {
    for (int i=0; i<NUM_OF_VNODES; i++) {
      ring.put(hash(serviceId + i), serviceId);
    }
  }

  private Long hash(String s) {
    messageDigest_.reset();
    messageDigest_.update(s.getBytes());
    byte[] digest = messageDigest_.digest();
    long h = 0;
    for (int i = 0; i < 4; i++) {
      h <<= 8;
      h |= ((int) digest[i]) & 0xFF;
    }
    return h;
  }

  private void removeNode(String serviceId) {
    for (int i=0; i<NUM_OF_VNODES; i++) {
      ring.remove(hash(serviceId + i));
    }
  }

  public synchronized String getServiceId(String objectKey) {
    Long objHash = hash(objectKey);
    LOG.debug("Input objectName: {}, hashValue: {}", objectKey, objHash);
    if (ring.containsKey(objHash)) {
      String serviceId = ring.get(objHash);
      LOG.debug("Returning serviceIdHash: {} serviceId : {}", objHash, serviceId);
      return serviceId;
    }
    SortedMap<Long, String> tailMap = ring.tailMap(objHash);
    Long nearestNodeId = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
    String serviceId = ring.get(nearestNodeId);
    LOG.debug("Nearest serviceIdHash: {} serviceId : {}", nearestNodeId, serviceId);
    return serviceId;
  }
}
