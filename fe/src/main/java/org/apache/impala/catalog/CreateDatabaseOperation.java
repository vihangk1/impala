package org.apache.impala.catalog;

import static org.apache.impala.service.CatalogOpExecutor.HMS_RPC_ERROR_FORMAT_STR;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDatabaseOperation extends CatalogDdlOperation {
  private final Database msDb_;
  private Db newDb_;
  private static final Logger LOG = LoggerFactory
      .getLogger(CreateDatabaseOperation.class);

  public CreateDatabaseOperation(CatalogServiceCatalog catalog, TDdlExecRequest request,
      TDdlExecResponse response, Database msDb) {
    super(catalog,request, response);
    this.msDb_ = Preconditions.checkNotNull(msDb);
  }

  @Override
  public void execute() throws CatalogException, ImpalaRuntimeException {
    TCreateDbParams params = request_.getCreate_db_params();
    String dbName = msDb_.getName();
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      try {
        msClient.getHiveClient().createDatabase(msDb_);
        // Load the database back from the HMS. It's unfortunate we need two
        // RPCs here, but otherwise we can't populate the location field of the
        // DB properly. We'll take the slight chance of a race over the incorrect
        // behavior of showing no location in 'describe database' (IMPALA-7439).
        Database db = msClient.getHiveClient().getDatabase(dbName);
        // TODO(Self-event) update using events
        newDb_ = catalog_.addDb(dbName, db);
        addSummary(response_, "Database has been created.");
      } catch (AlreadyExistsException e) {
        if (!params.if_not_exists) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "createDatabase"), e);
        }
        addSummary(response_, "Database already exists.");
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("Ignoring '%s' when creating database %s because " +
              "IF NOT EXISTS was specified.", e, dbName));
        }
        newDb_ = catalog_.getDb(dbName);
        if (newDb_ == null) {
          try {
            org.apache.hadoop.hive.metastore.api.Database msDb =
                msClient.getHiveClient().getDatabase(dbName);
            // No need to do a event based catalog update here since we didn't really
            // added the database in the HMS and hence we don't expect the event to be
            // generated.
            newDb_ = catalog_.addDb(dbName, msDb);
          } catch (TException e1) {
            throw new ImpalaRuntimeException(
                String.format(HMS_RPC_ERROR_FORMAT_STR, "createDatabase"), e1);
          }
        }
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "createDatabase"), e);
      }
    }
  }

  public Db getCreatedDb() {
    return newDb_;
  }
}
