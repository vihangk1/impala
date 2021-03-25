package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TAlterDbParams;
import org.apache.impala.thrift.TAlterDbSetOwnerParams;
import org.apache.impala.thrift.TAlterDbType;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;

public class AlterDatabaseOperation extends CatalogDdlOperation {

  // current we only support alter database owner
  private final TAlterDbSetOwnerParams params;
  // the db on which the alter operation is performed
  private Db db;
  // the new catalog version to be used to assign for this alter database operation
  private long newCatalogVersion;
  private String originalOwnerName;
  private PrincipalType originalOwnerType;
  // new owner and owner type
  private String newOwnerName;
  private PrincipalType newOwnerType;
  // updated HMS Database object
  private Database msDb;

  public AlterDatabaseOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response, CatalogOpExecutor catalogOpExecutor, boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
    Preconditions.checkNotNull(ddlExecRequest.getAlter_db_params());
    params = Preconditions
        .checkNotNull(ddlExecRequest.getAlter_db_params().set_owner_params);
  }

  @Override
  protected boolean requiresDdlLock() {
    return false;
  }

  @Override
  public void doHmsOperations() throws ImpalaException {
    Database msDbCopy = db.getMetaStoreDb().deepCopy();
    addCatalogServiceIdentifiers(catalog_, msDbCopy, newCatalogVersion);
    msDbCopy.setOwnerName(params.owner_name);
    msDbCopy.setOwnerType(PrincipalType.valueOf(params.owner_type.name()));
    try {
      applyAlterDatabase(msDbCopy);
      addSummary(response, "Updated database.");
    } catch (ImpalaRuntimeException e) {
      throw e;
    }
    msDb = msDbCopy.deepCopy();
  }

  @Override
  public void doCatalogOperations() throws ImpalaException {
    if (catalogOpExecutor_.getAuthzConfig().isEnabled()) {
      catalogOpExecutor_.getAuthzManager()
          .updateDatabaseOwnerPrivilege(params.server_name, db.getName(),
              originalOwnerName, originalOwnerType, newOwnerName,
              newOwnerType, response);
    }
    Db updatedDb = catalog_.updateDb(msDb);
    addDbToCatalogUpdate(updatedDb, wantMinimalResult, response.result);
    // now that HMS alter operation has succeeded, add this version to list of inflight
    // events in catalog database if event processing is enabled
    catalog_.addVersionsForInflightEvents(db, newCatalogVersion);
  }

  @Override
  public void init() throws ImpalaException {
    TAlterDbParams params = request.getAlter_db_params();
    Preconditions.checkNotNull(params);
    if (params.getAlter_type() != TAlterDbType.SET_OWNER) {
      throw new UnsupportedOperationException(
          "Unknown ALTER DATABASE operation type: " + params.getAlter_type());
    }
    String dbName = params.getDb();
    db = catalog_.getDb(dbName);
    if (db == null) {
      throw new CatalogException("Database: " + dbName + " does not exist.");
    }
    TAlterDbSetOwnerParams setOwnerParams = request.getAlter_db_params().set_owner_params;
    Preconditions.checkNotNull(setOwnerParams.owner_name);
    Preconditions.checkNotNull(setOwnerParams.owner_type);
    catalogOpExecutor_.tryLock(db, "altering the owner");
    // Get a new catalog version to assign to the database being altered.
    newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
    Database msDb = db.getMetaStoreDb();
    originalOwnerName = msDb.getOwnerName();
    originalOwnerType = msDb.getOwnerType();
    newOwnerName = msDb.getOwnerName();
    newOwnerType = msDb.getOwnerType();
  }

  @Override
  public void cleanUp() {
    db.getLock().unlock();
  }
}
