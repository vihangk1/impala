package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a new database in the metastore and adds the db name to the internal
 * metadata cache, marking its metadata to be lazily loaded on the next access.
 * Re-throws any Hive Meta Store exceptions encountered during the create, these
 * may vary depending on the Meta Store connection type (thrift vs direct db).
 * @param  syncDdl tells if SYNC_DDL option is enabled on this DDL request.
 */
public class CreateDatabaseOperation extends CatalogOperation {

  private static final Logger LOG = LoggerFactory
      .getLogger(CreateDatabaseOperation.class);
  // this flag is used to skip HMS operation if the db already exists in the catalog
  private boolean skipHmsOperation;
  @Nullable
  private Db existingDb;
  private TCreateDbParams params;
  private String dbName;
  // the new Db object which is created
  private Database msDb;
  private boolean takeDdlLock;

  public CreateDatabaseOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response, CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
  }

  @Override
  public boolean takeDdlLock() {
    return takeDdlLock;
  }

  @Override
  public void doHmsOperations() throws ImpalaException {
    if (skipHmsOperation) {
      addSummary(response, "Database already exists.");
      return;
    }

    org.apache.hadoop.hive.metastore.api.Database db =
        new org.apache.hadoop.hive.metastore.api.Database();
    db.setName(dbName);
    if (params.getComment() != null) {
      db.setDescription(params.getComment());
    }
    if (params.getLocation() != null) {
      db.setLocationUri(params.getLocation());
    }
    if (params.getManaged_location() != null) {
      db.setManagedLocationUri(params.getManaged_location());
    }
    db.setOwnerName(params.getOwner());
    db.setOwnerType(PrincipalType.USER);
    if (LOG.isTraceEnabled()) LOG.trace("Creating database " + dbName);
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      try {
        msClient.getHiveClient().createDatabase(db);
        // Load the database back from the HMS. It's unfortunate we need two
        // RPCs here, but otherwise we can't populate the location field of the
        // DB properly. We'll take the slight chance of a race over the incorrect
        // behavior of showing no location in 'describe database' (IMPALA-7439).
        msDb = msClient.getHiveClient().getDatabase(dbName);
        addSummary(response, "Database has been created.");
      } catch (AlreadyExistsException e) {
        if (!params.if_not_exists) {
          throw new ImpalaRuntimeException(
              String.format(HMS_RPC_ERROR_FORMAT_STR, "createDatabase"), e);
        }
        addSummary(response, "Database already exists.");
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("Ignoring '%s' when creating database %s because " +
              "IF NOT EXISTS was specified.", e, dbName));
        }
      } catch (TException e) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "createDatabase"), e);
      }
    }
  }

  @Override
  public void doCatalogOperations() throws ImpalaException {
    if (syncDdl && existingDb != null) {
      // When SYNC_DDL is enabled and the database already exists, we force a version
      // bump on it so that it is added to the next statestore update. Without this
      // we could potentially be referring to a database object that has already been
      // GC'ed from the TopicUpdateLog and waitForSyncDdlVersion() cannot find a
      // covering topic version (IMPALA-7961).
      //
      // This is a conservative hack to not break the SYNC_DDL semantics and could
      // possibly result in false-positive invalidates on this database. However,
      // that is better than breaking the SYNC_DDL semantics and the subsequent
      // queries referring to this database failing with "database not found" errors.
      long newVersion = catalog_.incrementAndGetCatalogVersion();
      existingDb.setCatalogVersion(newVersion);
      LOG.trace("Database {} version bumped to {} because SYNC_DDL is enabled.",
          existingDb.getName(), newVersion);
      return;
    }
    Db newDb = catalog_.getDb(dbName);
    if (newDb == null) {
      Preconditions.checkNotNull(msDb);
      newDb = catalog_.addDb(dbName, msDb);
    }
    addDbToCatalogUpdate(newDb, wantMinimalResult, response.result);
    if (catalogOpExecutor_.getAuthzConfig().isEnabled()) {
      catalogOpExecutor_.getAuthzManager().updateDatabaseOwnerPrivilege(params.server_name, newDb.getName(),
          /* oldOwner */ null, /* oldOwnerType */ null,
          newDb.getMetaStoreDb().getOwnerName(), newDb.getMetaStoreDb().getOwnerType(),
          response);
    }
  }

  @Override
  public void before() throws ImpalaException {
    params = Preconditions.checkNotNull(request.getCreate_db_params());
    dbName = params.getDb();
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name passed as argument to Catalog.createDatabase");
    Preconditions.checkState(!catalog_.isBlacklistedDb(dbName),
        String.format("Can't create blacklisted database: %s. %s", dbName,
            BLACKLISTED_DBS_INCONSISTENT_ERR_STR));
    existingDb = catalog_.getDb(dbName);
    if (params.if_not_exists && existingDb != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Skipping database creation because " + dbName + " already exists "
            + "and IF NOT EXISTS was specified.");
      }
      Preconditions.checkNotNull(existingDb);
      if (syncDdl) {
        catalogOpExecutor_.tryLock(existingDb, "create database");
      }
      // the database already exists and if not exists is true, hence we will skip HMS
      // operation
      skipHmsOperation = true;
    } else {
      // we are creating the database and hence we should take the ddl lock
      takeDdlLock = true;
    }
  }

  @Override
  public void after() {
    if (syncDdl && existingDb != null) {
      // Release the locks held in tryLock() in the before method
      catalog_.getLock().writeLock().unlock();
      existingDb.getLock().unlock();
    }
  }
}
