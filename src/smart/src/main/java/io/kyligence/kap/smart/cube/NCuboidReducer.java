package io.kyligence.kap.smart.cube;

import java.util.BitSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.model.ModelTree;

public class NCuboidReducer extends NAbstractCubeProposer {

    NCuboidReducer(NModelContext context) {
        super(context);
    }

    @Override
    void doPropose(NCubePlan cubePlan) {
        // get current cuboids
        Map<Pair<BitSet, BitSet>, NCuboidDesc> originalCuboids = Maps.newLinkedHashMap();
        for (NCuboidDesc cuboidDesc : cubePlan.getCuboids()) {
            Pair<BitSet, BitSet> key = new Pair<>(ImmutableBitSet.valueOf(cuboidDesc.getDimensions()).mutable(),
                    ImmutableBitSet.valueOf(cuboidDesc.getMeasures()).mutable());
            NCuboidDesc desc = originalCuboids.get(key);

            if (desc == null) {
                originalCuboids.put(key, cuboidDesc);
            } else {
                desc.getLayouts().addAll(cuboidDesc.getLayouts());
            }
        }
        
        // get to be removed cuboids
        Map<Pair<BitSet, BitSet>, NCuboidDesc> proposedCuboids = Maps.newLinkedHashMap();
        CuboidSuggester suggester = new CuboidSuggester(this.context, proposedCuboids);
        NDataModel model = context.getTargetModel();
        ModelTree modelTree = context.getModelTree();
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            Map<String, String> aliasMap = RealizationChooser.matches(model, ctx);
            ctx.fixModel(model, aliasMap);
            suggester.ingest(ctx, model);
            ctx.unfixModel();
        }
        
        // remove cuboids
        for (Entry<Pair<BitSet, BitSet>, NCuboidDesc> cuboidPair : proposedCuboids.entrySet()) {
            Pair<BitSet, BitSet> cuboidKey = cuboidPair.getKey();
            NCuboidDesc cuboid = cuboidPair.getValue();
            NCuboidDesc originalCuboid = originalCuboids.get(cuboidKey);
            if (originalCuboid == null) {
                continue;
            }
            for (NCuboidLayout cuboidLayout : cuboid.getLayouts()) {
                // TODO check if this layout is used by other query
                boolean hasExternalRef = false;
                if (hasExternalRef) {
                    continue;
                }
                NCuboidLayout toRemoveLayout = null;
                for (NCuboidLayout originalLayout : originalCuboid.getLayouts()) {
                    if (CuboidSuggester.compareLayouts(originalLayout, cuboidLayout)) {
                        toRemoveLayout = originalLayout;
                        break;
                    }
                }
                if (toRemoveLayout == null) {
                    continue;
                }
                originalCuboid.getLayouts().remove(toRemoveLayout);
            }
            if (originalCuboid.getLayouts().isEmpty()) {
                originalCuboids.remove(cuboidKey);
            }
        }
        
        cubePlan.setCuboids(Lists.newArrayList(originalCuboids.values()));
    }
}
 