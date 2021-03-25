package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TCreateDataSourceParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDatasourceOperation extends CatalogDdlOperation {

  private static final Logger LOG = LoggerFactory
      .getLogger(CreateDatasourceOperation.class);

  public CreateDatasourceOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response,
      CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
  }

  @Override
  protected boolean requiresDdlLock() {
    return false;
  }

  @Override
  protected void doHmsOperations() throws ImpalaException {
    // no-op
  }

  @Override
  protected void doCatalogOperations() throws ImpalaException {
    // TODO(IMPALA-7131): support data sources with LocalCatalog.
    TCreateDataSourceParams params = Preconditions
        .checkNotNull(request.create_data_source_params);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Adding DATA SOURCE: " + params.toString());
    }
    DataSource dataSource = DataSource.fromThrift(params.getData_source());
    DataSource existingDataSource = catalog_.getDataSource(dataSource.getName());
    if (existingDataSource != null) {
      if (!params.if_not_exists) {
        throw new ImpalaRuntimeException("Data source " + dataSource.getName() +
            " already exists.");
      }
      addSummary(response, "Data source already exists.");
      response.result.addToUpdated_catalog_objects(existingDataSource.toTCatalogObject());
      response.result.setVersion(existingDataSource.getCatalogVersion());
      return;
    }
    catalog_.addDataSource(dataSource);
    response.result.addToUpdated_catalog_objects(dataSource.toTCatalogObject());
    response.result.setVersion(dataSource.getCatalogVersion());
    addSummary(response, "Data source has been created.");
  }

  @Override
  protected void init() throws ImpalaException {
    // no-op
  }

  @Override
  protected void cleanUp() {
    // no-op
  }
}
