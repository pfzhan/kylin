package io.kyligence.kap.rest.config.initialize;

import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.source.jdbc.H2Database;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.rest.service.TableService;
import lombok.val;
import lombok.var;

public class ModelBrokenListenerTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    @Mock
    private TableService tableService = Mockito.spy(TableService.class);

    @InjectMocks
    private TableExtService tableExtService = Mockito.spy(new TableExtService());

    @Before
    public void setup() {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        // init DefaultScheduler
        SchedulerEventBusFactory.getInstance(getTestConfig()).register(modelBrokenListener);
        ReflectionTestUtils.setField(tableExtService, "tableService", tableService);

        try {
            setupPushdownEnv();
        } catch (Exception ignore) {
        }
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        val overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "9");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps,
                MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        projectManager.forceDropProject("broken_test");
        projectManager.forceDropProject("bad_query_test");
    }

    @After
    public void cleanup() throws Exception {
        SchedulerEventBusFactory.getInstance(getTestConfig()).unRegister(modelBrokenListener);
        cleanPushdownEnv();
        staticCleanupTestMetadata();
    }

    private void setupPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerJdbcImpl");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1", "sa", "");
        H2Database h2DB = new H2Database(h2Connection, getTestConfig(), "default");
        h2DB.loadAllTables();

        System.setProperty("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        System.setProperty("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        System.setProperty("kylin.query.pushdown.jdbc.username", "sa");
        System.setProperty("kylin.query.pushdown.jdbc.password", "");
    }

    private void cleanPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa", "");
        h2Connection.close();
        System.clearProperty("kylin.query.pushdown.jdbc.url");
        System.clearProperty("kylin.query.pushdown.jdbc.driver");
        System.clearProperty("kylin.query.pushdown.jdbc.username");
        System.clearProperty("kylin.query.pushdown.jdbc.password");
    }

    @Test
    public void testModelBrokenListener_DropModel() {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        System.setProperty("kylin.metadata.broken-model-deleted-on-smart-mode", "true");

        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT");

        await().atMost(60000, TimeUnit.MILLISECONDS).until(() -> modelManager.getDataModelDesc(modelId) == null);

        System.clearProperty("kylin.metadata.broken-model-deleted-on-smart-mode");
    }

    @Test
    public void testModelBrokenListener_TableOriented() throws Exception {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT");

        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(0,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
        });

        tableExtService.loadTables(new String[] { "DEFAULT.TEST_KYLIN_FACT" }, project);

        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(1,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
            Assert.assertEquals(2, EventDao.getInstance(getTestConfig(), project).getEventsByModel(modelId).size());
        });
    }

    @Test
    public void testModelBrokenListener_BrokenReason() throws Exception {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        var originModel = modelManager.getDataModelDesc(modelId);
        val copyForUpdate = modelManager.copyForWrite(originModel);
        copyForUpdate.setBrokenReason(NDataModel.BrokenReason.EVENT);
        modelManager.updateDataModelDesc(copyForUpdate);

        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(0,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
        });
    }

    @Test
    public void testModelBrokenListener_FullBuild() throws Exception {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        var originModel = modelManager.getDataModelDesc(modelId);
        val copyForUpdate = modelManager.copyForWrite(originModel);
        copyForUpdate.setPartitionDesc(null);
        modelManager.updateDataModelDesc(copyForUpdate);

        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT");
        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(0,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
        });

        tableExtService.loadTables(new String[] { "DEFAULT.TEST_KYLIN_FACT" }, project);

        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
            Assert.assertEquals(1, dataflow.getSegments().size());
            Assert.assertTrue(dataflow.getCoveredRange().isInfinite());
            Assert.assertEquals(2, EventDao.getInstance(getTestConfig(), project).getEventsByModel(modelId).size());
        });
    }
}
