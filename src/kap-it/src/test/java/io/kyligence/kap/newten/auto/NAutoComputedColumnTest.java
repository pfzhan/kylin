package io.kyligence.kap.newten.auto;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.newten.NAutoTestBase;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;

public class NAutoComputedColumnTest extends NAutoTestBase {

    @Test
    public void testComputedColumnsWontImpactFavoriteQuery() throws IOException {
        KylinConfig kylinConfig = getTestConfig();
        overwriteSystemProp("kylin.query.transformers", "io.kyligence.kap.query.util.ConvertToComputedColumn");
        // test all named columns rename
        String query = 
                "SELECT SUM(CASE WHEN PRICE > 100 THEN 100 ELSE PRICE END), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, "newten", new String[] {query});
        smartMaster.runAll();
        
        NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc computedColumnDesc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", computedColumnDesc.getColumnName());
        Assert.assertEquals("CASE WHEN TEST_KYLIN_FACT.PRICE > 100 THEN 100 ELSE TEST_KYLIN_FACT.PRICE END",
                computedColumnDesc.getExpression());
        Assert.assertEquals(2, model.getAllNamedColumns().size());
        Assert.assertEquals("CAL_DT", model.getAllNamedColumns().get(0).getName());
        Assert.assertEquals("CC_AUTO_1", model.getAllNamedColumns().get(1).getName());
        Assert.assertEquals(1, model.getEffectiveDimensions().size());
        Assert.assertEquals("CAL_DT", model.getEffectiveDimensions().get(0).getName());
        Measure measure = model.getEffectiveMeasures().get(1001);
        Assert.assertNotNull(measure);
        Assert.assertTrue(measure.getFunction().isSum());
        Assert.assertEquals("CC_AUTO_1", measure.getFunction().getParameter().getColRef().getName());
        
        NCubePlan cubePlan = smartMaster.getContext().getModelContexts().get(0).getTargetCubePlan();
        Assert.assertEquals(1, cubePlan.getAllCuboidLayouts().size());
        Assert.assertEquals(1, cubePlan.getAllCuboidLayouts().get(0).getId());
        
        // Assert query info is updated
        AccelerateInfo accelerateInfo = smartMaster.getContext().getAccelerateInfoMap().get(query);
        Assert.assertNotNull(accelerateInfo);
        Assert.assertFalse(accelerateInfo.isBlocked());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().size());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().iterator().next().getLayoutId());
    }
    
    // TODO add more detailed test case in #8285
}
