package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDropFunctionParams;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropFunctionOperation extends CatalogOperation {

  private static final Logger LOG = LoggerFactory.getLogger(DropFunctionOperation.class);
  private Db db;
  private final TDropFunctionParams params;
  private long newCatalogVersion;
  private FunctionName fName;
  private final ArrayList<TCatalogObject> removedFunctions = Lists.newArrayList();
  private boolean skipHMSAndCatalogOperation;

  public DropFunctionOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response,
      CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
    params = Preconditions.checkNotNull(ddlExecRequest.getDrop_fn_params());
  }

  @Override
  protected boolean takeDdlLock() {
    return false;
  }

  @Override
  protected void doHmsOperations() throws ImpalaException {
    if (skipHMSAndCatalogOperation) return;
    if (!params.isSetSignature()) {
      dropJavaFunctionFromHms(fName.getDb(), fName.getFunction(), params.if_exists);
      for (Function fn : db.getFunctions(fName.getFunction())) {
        if (fn.getBinaryType() != TFunctionBinaryType.JAVA
            || !fn.isPersistent()) {
          continue;
        }
        Preconditions.checkNotNull(catalog_.removeFunction(fn));
        removedFunctions.add(fn.toTCatalogObject());
      }
    } else {
      ArrayList<Type> argTypes = Lists.newArrayList();
      for (TColumnType t : params.arg_types) {
        argTypes.add(Type.fromThrift(t));
      }
      Function desc = new Function(fName, argTypes, Type.INVALID, false);
      //TODO(Vihang) this is different after refactor since we should not update the
      // catalog here; we are changing the flow here in the sense we will update HMS
      // first then remove the function in the catalog.
      Preconditions.checkState(db.getLock().isHeldByCurrentThread());
      Function fn = db.getFunction(desc, CompareMode.IS_INDISTINGUISHABLE);
      if (fn == null) {
        if (!params.if_exists) {
          throw new CatalogException(
              "Function: " + desc.signatureString() + " does not exist.");
        }
      } else {
        addCatalogServiceIdentifiers(db.getMetaStoreDb(),
            catalog_.getCatalogServiceId(), newCatalogVersion);
        // Flush DB changes to metastore
        applyAlterDatabase(db.getMetaStoreDb());
        removedFunctions.add(fn.toTCatalogObject());
        // now that HMS alter operation has succeeded, add this version to list of
        // inflight events in catalog database if event processing is enabled.
        catalog_.addVersionsForInflightEvents(db, newCatalogVersion);
      }
    }
  }

  /**
   * Drops the given function from Hive metastore. Returns true if successful and false if
   * the function does not exist and ifExists is true.
   */
  public void dropJavaFunctionFromHms(String db, String fn, boolean ifExists)
      throws ImpalaRuntimeException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().dropFunction(db, fn);
    } catch (NoSuchObjectException e) {
      if (!ifExists) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "dropFunction"), e);
      }
    } catch (TException e) {
      LOG.error("Error executing dropFunction() metastore call: " + fn, e);
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "dropFunction"), e);
    }
  }

  @Override
  protected void doCatalogOperations() throws ImpalaException {
    if (skipHMSAndCatalogOperation) return;
    if (!removedFunctions.isEmpty()) {
      addSummary(response, "Function has been dropped.");
      response.result.setRemoved_catalog_objects(removedFunctions);
    } else {
      addSummary(response, "Function does not exist.");
    }
    response.result.setVersion(catalog_.getCatalogVersion());
  }

  @Override
  protected void before() throws ImpalaException {
    fName = FunctionName.fromThrift(params.fn_name);
    db = catalog_.getDb(fName.getDb());
    if (db == null) {
      if (!params.if_exists) {
        throw new CatalogException("Database: " + fName.getDb()
            + " does not exist.");
      }
      addSummary(response, "Database does not exist.");
      skipHMSAndCatalogOperation = true;
      return;
    }

    catalogOpExecutor_.tryLock(db, "dropping function " + fName);
    // Get a new catalog version to assign to the database being altered. This is
    // needed for events processor as this method creates alter database events.
    newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
  }

  @Override
  protected void after() throws ImpalaException {
    if (db.getLock().isHeldByCurrentThread()) {
      db.getLock().unlock();
    }
  }
}
