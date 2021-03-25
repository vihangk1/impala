package org.apache.impala.catalog.operations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCreateFunctionParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.util.FunctionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateFunctionOperation extends CatalogDdlOperation {

  private static final Logger LOG = LoggerFactory
      .getLogger(CreateFunctionOperation.class);
  private TCreateFunctionParams params;
  private boolean isPersistentJavaFn;
  private long newCatalogVersion;
  private Db db;
  private boolean skipHmsOperation;
  private boolean skipCatalogOperation;
  private final List<TCatalogObject> addedFunctions = Lists.newArrayList();
  private Function fn;
  private boolean addedJavaFnToHms;
  private List<Function> funcs;

  public CreateFunctionOperation(TDdlExecRequest ddlExecRequest,
      TDdlExecResponse response, CatalogOpExecutor catalogOpExecutor,
      boolean wantMinimalResult) {
    super(ddlExecRequest, response, catalogOpExecutor, wantMinimalResult);
  }

  @Override
  protected boolean requiresDdlLock() {
    return false;
  }

  @Override
  protected void doHmsOperations() throws ImpalaException {
    if (skipHmsOperation) {
      return;
    }
    if (isPersistentJavaFn) {
      // For persistent Java functions we extract all supported function signatures from
      // the corresponding Jar and add each signature to the catalog.
      Preconditions.checkState(fn instanceof ScalarFunction);
      org.apache.hadoop.hive.metastore.api.Function hiveFn =
          ((ScalarFunction) fn).toHiveFunction();
      funcs = FunctionUtils.extractFunctions(fn.dbName(), hiveFn,
          BackendConfig.INSTANCE.getBackendCfg().local_library_path);
      if (funcs.isEmpty()) {
        throw new CatalogException(
            "No compatible function signatures found in class: " + hiveFn
                .getClassName());
      }
      addedJavaFnToHms = addJavaFunctionToHms(fn.dbName(), hiveFn, params.if_not_exists);
    }
  }

  /**
   * Creates a new function in the Hive metastore. Returns true if successful
   * and false if the call fails and ifNotExists is true.
   */
  public boolean addJavaFunctionToHms(String db,
      org.apache.hadoop.hive.metastore.api.Function fn, boolean ifNotExists)
      throws ImpalaRuntimeException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().createFunction(fn);
    } catch(AlreadyExistsException e) {
      if (!ifNotExists) {
        throw new ImpalaRuntimeException(
            String.format(HMS_RPC_ERROR_FORMAT_STR, "createFunction"), e);
      }
      return false;
    } catch (Exception e) {
      LOG.error("Error executing createFunction() metastore call: " +
          fn.getFunctionName(), e);
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "createFunction"), e);
    }
    return true;
  }

  @Override
  protected void doCatalogOperations() throws ImpalaException {
    if (skipCatalogOperation) {
      return;
    }
    if (isPersistentJavaFn) {
      if (addedJavaFnToHms) {
        Preconditions.checkState(!funcs.isEmpty());
        for (Function addedFn : funcs) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Adding function: %s.%s", addedFn.dbName(),
                addedFn.signatureString()));
          }
          Preconditions.checkState(catalog_.addFunction(addedFn));
          addedFunctions.add(addedFn.toTCatalogObject());
        }
      }
    } else {
      //TODO(Vihang): addFunction method below directly updates the database
      // parameters. If the applyAlterDatabase method below throws an exception,
      // catalog might end up in a inconsistent state. Ideally, we should make a copy
      // of hms Database object and then update the Db once the HMS operation succeeds
      // similar to what happens in alterDatabaseSetOwner method.
      if (catalog_.addFunction(fn)) {
        addCatalogServiceIdentifiers(catalog_, db.getMetaStoreDb(), newCatalogVersion);
        // Flush DB changes to metastore
        applyAlterDatabase(db.getMetaStoreDb());
        addedFunctions.add(fn.toTCatalogObject());
        // now that HMS alter database has succeeded, add this version to list of
        // inflight events in catalog database if event processing is enabled.
        catalog_.addVersionsForInflightEvents(db, newCatalogVersion);
      }
    }
    if (!addedFunctions.isEmpty()) {
      response.result.setUpdated_catalog_objects(addedFunctions);
      response.result.setVersion(catalog_.getCatalogVersion());
      addSummary(response, "Function has been created.");
    } else {
      addSummary(response, "Function already exists.");
    }
  }

  @Override
  protected void init() throws ImpalaException {
    params = Preconditions.checkNotNull(request.create_fn_params);
    fn = Function.fromThrift(params.getFn());
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Adding %s: %s",
          fn.getClass().getSimpleName(), fn.signatureString()));
    }
    isPersistentJavaFn =
        (fn.getBinaryType() == TFunctionBinaryType.JAVA) && fn.isPersistent();
    db = catalog_.getDb(fn.dbName());
    if (db == null) {
      throw new CatalogException("Database: " + fn.dbName() + " does not exist.");
    }

    catalogOpExecutor_.tryLock(db, "creating function " + fn.getClass().getSimpleName());
    // Get a new catalog version to assign to the database being altered. This is
    // needed for events processor as this method creates alter database events.
    newCatalogVersion = catalog_.incrementAndGetCatalogVersion();
    catalog_.getLock().writeLock().unlock();
    // Search for existing functions with the same name or signature that would
    // conflict with the function being added.
    for (Function function : db.getFunctions(fn.functionName())) {
      if (isPersistentJavaFn || (function.isPersistent() &&
          (function.getBinaryType() == TFunctionBinaryType.JAVA)) ||
          function.compare(fn, Function.CompareMode.IS_INDISTINGUISHABLE)) {
        if (!params.if_not_exists) {
          throw new CatalogException("Function " + fn.functionName() +
              " already exists.");
        }
        addSummary(response, "Function already exists.");
        skipCatalogOperation = true;
        skipHmsOperation = true;
        return;
      }
    }
  }

  @Override
  protected void cleanUp() {
    db.getLock().unlock();
  }
}
