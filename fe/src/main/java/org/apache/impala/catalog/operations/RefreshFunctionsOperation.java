package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TResetMetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RefreshFunctionsOperation extends CatalogOperation {

  private static final Logger LOG = LoggerFactory
      .getLogger(RefreshFunctionsOperation.class);
  private final TResetMetadataRequest request_;
  private final TResetMetadataResponse response_;
  private final CatalogOpExecutor catalogOpExecutor_;
  private final CatalogServiceCatalog catalog_;

  public RefreshFunctionsOperation(CatalogOpExecutor catalogOpExecutor,
      TResetMetadataRequest req,
      TResetMetadataResponse response) {
    this.catalogOpExecutor_ = Preconditions.checkNotNull(catalogOpExecutor);
    this.catalog_ = catalogOpExecutor_.getCatalog();
    this.request_ = Preconditions.checkNotNull(req);
    this.response_ = Preconditions.checkNotNull(response);
  }

  @Override
  protected boolean requiresDdlLock() {
    return true;
  }

  @Override
  protected void doHmsOperations() throws ImpalaException {
    // there is nothing to do here.
  }

  @Override
  protected void doCatalogOperations() throws CatalogException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      List<TCatalogObject> addedFuncs = Lists.newArrayList();
      List<TCatalogObject> removedFuncs = Lists.newArrayList();
      catalog_.refreshFunctions(msClient, request_.getDb_name(), addedFuncs, removedFuncs);
      response_.result.setUpdated_catalog_objects(addedFuncs);
      response_.result.setRemoved_catalog_objects(removedFuncs);
      response_.result.setVersion(catalog_.getCatalogVersion());
      for (TCatalogObject removedFn: removedFuncs) {
        catalog_.getDeleteLog().addRemovedObject(removedFn);
      }
    }
  }

  @Override
  protected void init() throws ImpalaException {
    Preconditions.checkState(!catalog_.isBlacklistedDb(request_.getDb_name()),
        String.format("Can't refresh functions in blacklisted database: %s. %s",
            request_.getDb_name(),
            CatalogDdlOperation.BLACKLISTED_DBS_INCONSISTENT_ERR_STR));
  }

  @Override
  protected void cleanUp() throws ImpalaException {
    //nothing to do here
  }
}
