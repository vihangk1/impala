package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.impala.analysis.TableName;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TCreateOrAlterViewParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a new view in the metastore and adds an entry to the metadata cache to
 * lazily load the new metadata on the next access. Re-throws any Metastore
 * exceptions encountered during the create.
 */
public class CreateViewOperation extends CreateTableOperation {
  private static final Logger LOG = LoggerFactory.getLogger(CreateViewOperation.class);

  public CreateViewOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response,
      CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
  }

  @Override
  protected void init() throws ImpalaException {
    TCreateOrAlterViewParams params = request.create_view_params;
    TableName tableName = TableName.fromThrift(params.getView_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
            params.getColumns().size() > 0,
        "Null or empty column list given as argument to DdlExecutor.createView");
    Preconditions.checkState(!catalog_.isBlacklistedTable(tableName),
        String.format("Can't create view with blacklisted table name: %s. %s", tableName,
            BLACKLISTED_TABLES_INCONSISTENT_ERR_STR));
    if (params.if_not_exists &&
        catalog_.containsTable(tableName.getDb(), tableName.getTbl())) {
      LOG.trace(String.format("Skipping view creation because %s already exists and " +
          "ifNotExists is true.", tableName));
    }

    // Create new view.
    org.apache.hadoop.hive.metastore.api.Table view =
        new org.apache.hadoop.hive.metastore.api.Table();
    setCreateViewAttributes(params, view);
    // set newTable from the base class so that we can create it.
    newTable = view;
    LOG.trace(String.format("Creating view %s", tableName));
  }

  /**
   * Sets the given params in the metastore table as appropriate for a
   * create view operation.
   */
  private void setCreateViewAttributes(TCreateOrAlterViewParams params,
      org.apache.hadoop.hive.metastore.api.Table view) {
    view.setTableType(TableType.VIRTUAL_VIEW.toString());
    view.setViewOriginalText(params.getOriginal_view_def());
    view.setViewExpandedText(params.getExpanded_view_def());
    view.setDbName(params.getView_name().getDb_name());
    view.setTableName(params.getView_name().getTable_name());
    view.setOwner(params.getOwner());
    if (view.getParameters() == null) view.setParameters(new HashMap<String, String>());
    if (params.isSetComment() && params.getComment() != null) {
      view.getParameters().put("comment", params.getComment());
    }
    StorageDescriptor sd = new StorageDescriptor();
    // Add all the columns to a new storage descriptor.
    sd.setCols(buildFieldSchemaList(params.getColumns()));
    // Set a dummy SerdeInfo for Hive.
    sd.setSerdeInfo(new SerDeInfo());
    view.setSd(sd);
  }

}
