package org.apache.shardingsphere.mode.manager.cluster;

import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.instance.InstanceContext;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereTable;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereView;
import org.apache.shardingsphere.infra.metadata.database.schema.pojo.AlterSchemaMetaDataPOJO;
import org.apache.shardingsphere.infra.metadata.database.schema.pojo.AlterSchemaPOJO;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.mode.metadata.persist.MetaDataPersistService;
import org.apache.shardingsphere.mode.persist.PersistRepository;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class ClusterModeContextManagerTest {

    private PersistRepository repository;
    private ClusterModeContextManager clusterModeContextManager;
    private ContextManager contextManager;


    @Before
    public void setUp() {
        repository = mock(PersistRepository.class);
        MetaDataPersistService metaDataPersistService = new MetaDataPersistService(repository);
        MetaDataContexts metaDataContexts = new MetaDataContexts(metaDataPersistService, new ShardingSphereMetaData());
        InstanceContext instanceContext = mock(InstanceContext.class);
        contextManager = new ContextManager(metaDataContexts, instanceContext);
        clusterModeContextManager = new ClusterModeContextManager();
        clusterModeContextManager.setContextManagerAware(contextManager);
    }

    @Test
    public void assertCreateDatabase() {
        clusterModeContextManager.createDatabase("foo_db");
        verify(repository).persist(eq("/metadata/foo_db"), anyString());
    }

    @Test
    public void assertDropDatabase() {
        clusterModeContextManager.dropDatabase("foo_db");
        verify(repository).delete("/metadata/foo_db");
    }

    @Test
    public void assertCreateSchemas() {
        clusterModeContextManager.createSchema("foo_db", "foo_schema");
        verify(repository).persist(eq("/metadata/foo_db/schemas/foo_schema/tables"), anyString());
    }

    @Test
    public void assertDropSchema() {
        clusterModeContextManager.dropSchema("foo_db", Collections.singletonList("foo_schema"));
        verify(repository).delete("/metadata/foo_db/schemas/foo_schema");
    }

    @Test
    public void assertAlterSchema() {
        ShardingSphereMetaData shardingSphereMetaData = contextManager.getMetaDataContexts().getMetaData();
        DatabaseType databaseType = mock(DatabaseType.class);
        shardingSphereMetaData.addDatabase("foo_db", databaseType);

        ShardingSphereSchema shardingSphereSchema = mock(ShardingSphereSchema.class);
        shardingSphereMetaData.getDatabase("foo_db").putSchema("foo_schema", shardingSphereSchema);

        AlterSchemaPOJO alterSchemaPOJO = new AlterSchemaPOJO("foo_db", "foo", "foo_schema", "bar_schema");
        clusterModeContextManager.alterSchema(alterSchemaPOJO);

        verify(repository).persist(eq("/metadata/foo_db/schemas/bar_schema/tables"), anyString());
        verify(repository).delete(eq("/metadata/foo_db/schemas/foo_schema"));
    }

    @Test
    public void assertAlteredSchemaMetaData_AlteredTable() {
        AlterSchemaMetaDataPOJO metaDataPOJO = new AlterSchemaMetaDataPOJO("foo_db", "foo_schema", "foo_source");
        metaDataPOJO.getAlteredTables().add(new ShardingSphereTable("foo_table", Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList()));
        clusterModeContextManager.alterSchemaMetaData(metaDataPOJO);
        verify(repository).persist(eq("/metadata/foo_db/schemas/foo_schema/tables/foo_table"),
                eq("name: foo_table" + System.lineSeparator()));
    }

    @Test
    public void assertAlteredSchemaMetaData_AlteredViews() {
        AlterSchemaMetaDataPOJO metaDataPOJO = new AlterSchemaMetaDataPOJO("foo_db", "foo_schema", "foo_source");
        metaDataPOJO.getAlteredViews().add(new ShardingSphereView("foo_view", ""));
        clusterModeContextManager.alterSchemaMetaData(metaDataPOJO);
        verify(repository).persist(eq("/metadata/foo_db/schemas/foo_schema/views/foo_view"),
                eq("name: foo_view" + System.lineSeparator() + "viewDefinition: ''" + System.lineSeparator()));
    }


    @Test
    public void assertAlteredSchemaMetaData_DroppedTable() {
        AlterSchemaMetaDataPOJO metaDataPOJO = new AlterSchemaMetaDataPOJO("foo_db", "foo_schema", "foo_source");
        metaDataPOJO.getDroppedTables().add("foo_table");
        clusterModeContextManager.alterSchemaMetaData(metaDataPOJO);
        verify(repository).delete(eq("/metadata/foo_db/schemas/foo_schema/tables/foo_table"));
    }

    @Test
    public void assertAlteredSchemaMetaData_DroppedView() {
        AlterSchemaMetaDataPOJO metaDataPOJO = new AlterSchemaMetaDataPOJO("foo_db", "foo_schema", "foo_source");
        metaDataPOJO.getDroppedViews().add("foo_view");
        clusterModeContextManager.alterSchemaMetaData(metaDataPOJO);
        verify(repository).delete(eq("/metadata/foo_db/schemas/foo_schema/views/foo_view"));
    }





}
