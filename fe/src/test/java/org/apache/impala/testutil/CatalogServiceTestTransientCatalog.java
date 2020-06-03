package org.apache.impala.testutil;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.impala.authorization.NoopAuthorizationFactory.NoopAuthorizationManager;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.compat.MetastoreShim;
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
