package org.apache.impala.catalog;

import static org.apache.impala.service.CatalogOpExecutor.HMS_RPC_ERROR_FORMAT_STR;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.thrift.TException;

public class DropDatabaseOperation extends CatalogDdlOperation {

  private final String dbName_;
  private Db removedDb_;

  public DropDatabaseOperation(CatalogServiceCatalog catalog, TDdlExecRequest request,
      TDdlExecResponse response, String dbName) {
    super(catalog, request, response);
    dbName_ = Preconditions.checkNotNull(dbName);
  }

  @Override
  protected void execute() throws CatalogException, ImpalaRuntimeException {
    // The Kudu tables in the HMS should have been dropped at this point
    // with the Hive Metastore integration enabled.
    TDropDbParams params = request_.getDrop_db_params();
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      // HMS client does not have a way to identify if the database was dropped or
      // not if the ignoreIfUnknown flag is true. Hence we always pass the
      // ignoreIfUnknown as false and catch the NoSuchObjectFoundException and
      // determine if we should throw or not
      msClient.getHiveClient().dropDatabase(
          dbName_, /* deleteData */true, /* ignoreIfUnknown */false,
          params.cascade);
      addSummary(response_, "Database has been dropped.");
    } catch (TException e) {
      if (e instanceof NoSuchObjectException && params.if_exists) {
        // if_exists param was set; we ignore the NoSuchObjectFoundException
        addSummary(response_, "Database does not exist.");
      } else {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "dropDatabase"), e);
      }
    }
    // TODO(Self-event) update using events
    removedDb_ = catalog_.removeDb(dbName_);
  }

  public Db getRemovedDb() {
    return removedDb_;
  }
}
