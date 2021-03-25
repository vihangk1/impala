package org.apache.impala.catalog.operations;

import org.apache.impala.common.ImpalaException;

public abstract class CatalogOperation {
  // Lock used to ensure that CREATE[DROP] TABLE[DATABASE] operations performed in
  // catalog_ and the corresponding RPC to apply the change in HMS are atomic.
  private static final Object metastoreDdlLock_ = new Object();

  protected abstract boolean requiresDdlLock();

  protected abstract void doHmsOperations() throws ImpalaException;

  protected abstract void doCatalogOperations() throws ImpalaException;

  protected abstract void init() throws ImpalaException;

  protected abstract void cleanUp() throws ImpalaException;

  public void execute() throws ImpalaException {
    try {
      init();
      if (requiresDdlLock()) {
        synchronized (metastoreDdlLock_) {
          doHmsOperations();
          doCatalogOperations();
        }
      } else {
        doHmsOperations();
        doCatalogOperations();
      }
    } finally {
      cleanUp();
    }
  }
}
