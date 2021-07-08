package org.apache.impala.catalog.metastore;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

//TODO create separate annotation for DDLs which invalidate cache
// and read-only APIs which use catalogd cache.
@Retention(RetentionPolicy.RUNTIME)
public @interface ServedFromCatalogd {
}