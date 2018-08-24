package io.kyligence.kap.smart;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;

public class NModelShrinkProposer extends NAbstractProposer {

    public NModelShrinkProposer(NSmartContext modelCtx) {
        super(modelCtx);
    }

    @Override
    void propose() {
        if (context.getModelContexts() == null)
            return;

        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            if (modelCtx.getOrigModel() == null) {
                continue;
            }
            if (modelCtx.getOrigCubePlan() == null) {
                continue;
            }
            if (modelCtx.getTargetCubePlan() == null) {
                continue;
            }
            
            NDataModel model = modelCtx.getTargetModel();
            Map<Integer, NamedColumn> namedColumns = new HashMap<>();
            for (NamedColumn namedColumn : model.getAllNamedColumns()) {
                namedColumn.tomb = true;
                namedColumns.put(namedColumn.id, namedColumn);
            }
            Map<Integer, Measure> measures = new HashMap<>(); 
            for (Measure measure : model.getAllMeasures()) {
                measure.tomb = true;
                measures.put(measure.id, measure);
            }
            
            NCubePlan cubePlan = modelCtx.getTargetCubePlan();
            for (NCuboidDesc cuboidDesc : cubePlan.getCuboids()) {
                for (int id : cuboidDesc.getDimensions()) {
                    NamedColumn used = namedColumns.get(id);
                    if (used != null) {
                        used.tomb = false;
                    }
                }
                for (int id : cuboidDesc.getMeasures()) {
                    Measure used = measures.get(id);
                    if (used != null) {
                        used.tomb = false;
                    }
                }
            }
        }
        
        throw new NotImplementedException();
    }

}
