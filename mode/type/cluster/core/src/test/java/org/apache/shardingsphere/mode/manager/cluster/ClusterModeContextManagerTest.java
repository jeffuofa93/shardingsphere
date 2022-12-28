package org.apache.shardingsphere.mode.manager.cluster;

import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.instance.InstanceContext;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereTable;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereView;
import org.apache.shardingsphere.infra.metadata.database.schema.pojo.AlterSchemaPOJO;
import org.apache.shardingsphere.infra.util.yaml.YamlEngine;
import org.apache.shardingsphere.infra.yaml.schema.pojo.YamlShardingSphereTable;
import org.apache.shardingsphere.infra.yaml.schema.swapper.YamlTableSwapper;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.mode.metadata.persist.MetaDataPersistService;
import org.apache.shardingsphere.mode.metadata.persist.service.DatabaseMetaDataPersistService;
import org.apache.shardingsphere.mode.persist.PersistRepository;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;


public class ClusterModeContextManagerTest {

    private PersistRepository repository;
    private ContextManager contextManager;


    @Before
    public void setUp() {
        repository = mock(PersistRepository.class);
        MetaDataPersistService metaDataPersistService = new MetaDataPersistService(repository );
        MetaDataContexts metaDataContexts = new MetaDataContexts(metaDataPersistService,new ShardingSphereMetaData());
        InstanceContext instanceContext = mock(InstanceContext.class);
        this.contextManager = new ContextManager(metaDataContexts,instanceContext);
    }

    @Test
    public void assertCreateDatabase(){
        ClusterModeContextManager clusterModeContextManager = new ClusterModeContextManager();
        clusterModeContextManager.setContextManagerAware(contextManager);
        clusterModeContextManager.createDatabase("foo_db");
        verify(repository).persist(eq("/metadata/foo_db"),anyString());
    }

    @Test
    public void assertDropDatabase(){
        ClusterModeContextManager clusterModeContextManager = new ClusterModeContextManager();
        clusterModeContextManager.setContextManagerAware(contextManager);
        clusterModeContextManager.dropDatabase("foo_db");
        verify(repository).delete("/metadata/foo_db");
    }

    @Test
    public void assertCreateSchemas() {
        ClusterModeContextManager clusterModeContextManager = new ClusterModeContextManager();
        clusterModeContextManager.setContextManagerAware(contextManager);
        clusterModeContextManager.createSchema("foo_db", "foo_schema");
        verify(repository).persist(eq("/metadata/foo_db/schemas/foo_schema/tables"), anyString());
    }

    @Test
    public void assertDropSchema() {
        ClusterModeContextManager clusterModeContextManager = new ClusterModeContextManager();
        clusterModeContextManager.setContextManagerAware(contextManager);
        clusterModeContextManager.dropSchema("foo_db", Collections.singletonList("foo_schema"));
        verify(repository).delete("/metadata/foo_db/schemas/foo_schema");
    }

    @Test
    public void assertAlterSchema() {
        ClusterModeContextManager clusterModeContextManager = new ClusterModeContextManager();
        clusterModeContextManager.setContextManagerAware(contextManager);
        ShardingSphereMetaData shardingSphereMetaData = contextManager.getMetaDataContexts().getMetaData();
        DatabaseType databaseType = mock(DatabaseType.class);
        shardingSphereMetaData.addDatabase("foo_db",databaseType);
        ShardingSphereSchema shardingSphereSchema = mock(ShardingSphereSchema.class);
        shardingSphereMetaData.getDatabase("foo_db").putSchema("foo_schema",shardingSphereSchema);
        AlterSchemaPOJO alterSchemaPOJO = new AlterSchemaPOJO("foo_db","foo","foo_schema","bar_schema");

        clusterModeContextManager.alterSchema(alterSchemaPOJO);
        verify(repository).persist(eq("/metadata/foo_db/schemas/bar_schema/tables"), anyString());
        verify(repository).delete(eq("/metadata/foo_db/schemas/foo_schema"));
    }

    @Test
    public void assertAlterSchemaMetaData() {
        ClusterModeContextManager clusterModeContextManager = new ClusterModeContextManager();
        clusterModeContextManager.setContextManagerAware(contextManager);
        ShardingSphereMetaData shardingSphereMetaData = contextManager.getMetaDataContexts().getMetaData();
        DatabaseType databaseType = mock(DatabaseType.class);
        shardingSphereMetaData.addDatabase("foo_db",databaseType);
        ShardingSphereSchema shardingSphereSchema = mock(ShardingSphereSchema.class);
        shardingSphereMetaData.getDatabase("foo_db").putSchema("foo_schema",shardingSphereSchema);
        AlterSchemaPOJO alterSchemaPOJO = new AlterSchemaPOJO("foo_db","foo","foo_schema","bar_schema");

        clusterModeContextManager.alterSchema(alterSchemaPOJO);
        verify(repository).persist(eq("/metadata/foo_db/schemas/bar_schema/tables"), anyString());
        verify(repository).delete(eq("/metadata/foo_db/schemas/foo_schema"));
    }



//    @Test
//    public void assertPersistSchemaMetaData() {
//        ShardingSphereTable table = new ShardingSphereTable("FOO_TABLE", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
//        ShardingSphereView view = new ShardingSphereView("FOO_VIEW", "select id from foo_table");
//        new DatabaseMetaDataPersistService(repository).persist("foo_db", "foo_schema",
//                new ShardingSphereSchema(Collections.singletonMap("FOO_TABLE", table), Collections.singletonMap("FOO_VIEW", view)));
//        verify(repository).persist(eq("/metadata/foo_db/schemas/foo_schema/tables/foo_table"), anyString());
//    }
//
//    @Test
//    public void assertLoadSchemas() {
//        DatabaseMetaDataPersistService databaseMetaDataPersistService = new DatabaseMetaDataPersistService(repository);
//        when(repository.getChildrenKeys("/metadata/foo_db/schemas")).thenReturn(Collections.singletonList("foo_schema"));
//        when(repository.getChildrenKeys("/metadata/foo_db/schemas/foo_schema/tables")).thenReturn(Collections.singletonList("t_order"));
//        when(repository.getDirectly("/metadata/foo_db/schemas/foo_schema/tables/t_order")).thenReturn(readYAML());
//        Map<String, ShardingSphereSchema> schema = databaseMetaDataPersistService.loadSchemas("foo_db");
//        assertThat(schema.size(), is(1));
//        Map<String, ShardingSphereSchema> empty = databaseMetaDataPersistService.loadSchemas("test");
//        assertThat(empty.size(), is(0));
//        assertThat(schema.get("foo_schema").getAllTableNames(), is(Collections.singleton("t_order")));
//        assertThat(schema.get("foo_schema").getTable("t_order").getIndexes().keySet(), is(Collections.singleton("primary")));
//        assertThat(schema.get("foo_schema").getAllColumnNames("t_order").size(), is(1));
//        assertThat(schema.get("foo_schema").getTable("t_order").getColumns().keySet(), is(Collections.singleton("id")));
//    }
//
//    @SneakyThrows({IOException.class, URISyntaxException.class})
//    private String readYAML() {
//        return Files.readAllLines(Paths.get(ClassLoader.getSystemResource("yaml/schema/table.yaml").toURI())).stream().map(each -> each + System.lineSeparator()).collect(Collectors.joining());
//    }




}
