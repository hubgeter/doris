// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.qe.ConnectContext;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.View;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

class IcebergViewNamespaceTest {
    @Test
    void readsNamespaceAndSchemaFromSavedVersion() {
        IcebergExternalTable.ViewDefinition definition = loadDefinition(null, Namespace.of("src"));

        Assertions.assertEquals("iceberg", definition.getDefaultCatalog());
        Assertions.assertEquals("src", definition.getDefaultDatabase());
        Assertions.assertEquals("SELECT x FROM t", definition.getSql());
        Assertions.assertEquals("declared", definition.getSchema().get(0).getName());
    }

    @Test
    void keepsExplicitCrossCatalogNamespace() {
        IcebergExternalTable.ViewDefinition definition = loadDefinition("other", Namespace.of("a", "b"));

        Assertions.assertEquals("other", definition.getDefaultCatalog());
        Assertions.assertEquals("a.b", definition.getDefaultDatabase());
    }

    @Test
    void projectsTheRootOnlyWhenDatabaseLookupRoundTrips() {
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getProperties()).thenReturn(
                Collections.singletonMap(IcebergExternalCatalog.EXTERNAL_CATALOG_NAME, "root"));
        IcebergMetadataOps ops = new IcebergMetadataOps(catalog, Mockito.mock(RESTCatalog.class));

        Assertions.assertEquals(Namespace.of("db"), ops.visibleViewNamespace(Namespace.of("db", "root")));
        Assertions.assertEquals(Namespace.of("root", "db"), ops.visibleViewNamespace(Namespace.of("root", "db")));
    }

    @Test
    void restoresTheCallerNamespaceAfterNestedExpansion() {
        ConnectContext context = new ConnectContext();
        ConnectContext.NameSpaceContext caller = new ConnectContext.NameSpaceContext("iceberg", "store", 42L);
        context.restoreNameSpaceContext(caller);

        context.setViewDefaultNamespace("iceberg", "source");
        ConnectContext.NameSpaceContext outerView = context.getNameSpaceContext();
        context.setViewDefaultNamespace("other", "");
        Assertions.assertEquals(-1L, context.getCurrentDbId());
        context.restoreNameSpaceContext(outerView);
        Assertions.assertEquals("source", context.getDatabase());
        context.restoreNameSpaceContext(caller);
        Assertions.assertEquals("iceberg", context.getDefaultCatalog());
        Assertions.assertEquals("store", context.getDatabase());
        Assertions.assertEquals(42L, context.getCurrentDbId());
    }

    private IcebergExternalTable.ViewDefinition loadDefinition(String catalogName, Namespace namespace) {
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("iceberg");
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {});
        IcebergMetadataOps ops = new IcebergMetadataOps(catalog, Mockito.mock(RESTCatalog.class));
        Mockito.when(catalog.getMetadataOps()).thenReturn(ops);
        IcebergExternalDatabase database = new IcebergExternalDatabase(catalog, 2L, "store", "store");
        IcebergExternalTable table = Mockito.spy(new IcebergExternalTable(3L, "v", "v", catalog, database));
        View view = Mockito.mock(View.class);
        ImmutableViewVersion.Builder version = ImmutableViewVersion.builder()
                .versionId(1).timestampMillis(1).schemaId(4)
                .defaultNamespace(namespace)
                .addRepresentations(ImmutableSQLViewRepresentation.builder()
                        .dialect("spark").sql("SELECT x FROM t").build());
        if (catalogName != null) {
            version.defaultCatalog(catalogName);
        }
        Mockito.when(view.currentVersion()).thenReturn(version.build());
        Mockito.when(view.schemas()).thenReturn(Collections.singletonMap(4, new Schema(4,
                Types.NestedField.required(1, "declared", Types.IntegerType.get()))));
        Mockito.doReturn(view).when(table).getIcebergView();

        return table.getViewDefinition();
    }
}
