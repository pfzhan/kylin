/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.metadata.cube.utils;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;

public class IndexPlanReduceUtilTest {

    private IndexEntity aggIndex1, aggIndex2, aggIndex3, aggIndex4, aggIndex5, aggIndex6;
    private IndexEntity tableIndex1, tableIndex2, tableIndex3, tableIndex4;

    @Test
    public void testCollectRedundantAggIndexLayoutsForGarbageCleaning() {
        initAggIndex();
        // case 1: index1.dimensions == index2.dimensions && index2.measures contains index1.measures
        Map<LayoutEntity, LayoutEntity> map1 = IndexPlanReduceUtil
                .collectRedundantLayoutsOfAggIndex(Lists.newArrayList(aggIndex1, aggIndex2), true);
        Assert.assertEquals(1, map1.size());
        LayoutEntity redundant1 = map1.keySet().iterator().next();
        LayoutEntity reserved1 = map1.get(redundant1);
        Assert.assertEquals("[1, 2, 3, 4, 100000, 100003]", redundant1.getColOrder().toString());
        Assert.assertEquals("[1, 2, 3, 4, 100000, 100002, 100003]", reserved1.getColOrder().toString());

        // case 2: index1.dimensions == index3.dimensions && index1.measures != index3.measures
        Map<LayoutEntity, LayoutEntity> map2 = IndexPlanReduceUtil
                .collectRedundantLayoutsOfAggIndex(Lists.newArrayList(aggIndex1, aggIndex3), true);
        Assert.assertTrue(map2.isEmpty());

        // case 3: index3.dimensions != index4.dimensions && index3.measures == index4.measures
        Map<LayoutEntity, LayoutEntity> map3 = IndexPlanReduceUtil
                .collectRedundantLayoutsOfAggIndex(Lists.newArrayList(aggIndex3, aggIndex4), true);
        Assert.assertTrue(map3.isEmpty());

        // case 4: index5.dimensions == index6.dimensions == 0 && index5.measures contains index6.measures
        Map<LayoutEntity, LayoutEntity> map4 = IndexPlanReduceUtil
                .collectRedundantLayoutsOfAggIndex(Lists.newArrayList(aggIndex5, aggIndex6), true);
        Assert.assertEquals(1, map4.size());
        LayoutEntity redundant4 = map4.keySet().iterator().next();
        LayoutEntity reserved4 = map4.get(redundant4);
        Assert.assertEquals("[100000]", redundant4.getColOrder().toString());
        Assert.assertEquals("[100000, 100001]", reserved4.getColOrder().toString());

        // a case may not happen at present
        LayoutEntity layout1 = new LayoutEntity();
        layout1.setId(1);
        layout1.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100001));
        layout1.setShardByColumns(Lists.newArrayList(1));
        LayoutEntity layout2 = new LayoutEntity();
        layout2.setId(2);
        layout2.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100001));
        layout2.setShardByColumns(Lists.newArrayList(2));
        IndexEntity entity = new IndexEntity();
        entity.setLayouts(Lists.newArrayList(layout1, layout2));
        entity.setMeasures(Lists.newArrayList(100000, 100001));
        entity.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        entity.setId(calIndexId(entity));
        Map<LayoutEntity, LayoutEntity> map5 = IndexPlanReduceUtil
                .collectRedundantLayoutsOfAggIndex(Lists.newArrayList(entity), true);
        Assert.assertTrue(map5.isEmpty());
    }

    @Test
    public void testCollectRedundantAggIndexLayouts() {
        LayoutEntity layout1 = new LayoutEntity(); // a proposed layout
        layout1.setId(1);
        layout1.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100001));
        layout1.setInProposing(false);
        LayoutEntity layout2 = new LayoutEntity(); // a proposing layout
        layout2.setId(10001);
        layout2.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100001, 100002));
        layout2.setInProposing(true);
        LayoutEntity layout3 = new LayoutEntity(); // a proposing layout
        layout3.setId(10002);
        layout3.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000, 100001, 100002));
        layout3.setInProposing(true);
        LayoutEntity layout4 = new LayoutEntity(); // a proposing layout
        layout4.setId(20001);
        layout4.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000, 100002));
        layout4.setInProposing(true);
        IndexEntity entity1 = new IndexEntity();
        entity1.setLayouts(Lists.newArrayList(layout1));
        entity1.setMeasures(Lists.newArrayList(100000, 100001));
        entity1.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        entity1.setId(calIndexId(entity1));
        IndexEntity entity2 = new IndexEntity();
        entity2.setLayouts(Lists.newArrayList(layout2, layout3));
        entity2.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        entity2.setMeasures(Lists.newArrayList(100000, 100001, 100002));
        entity2.setId(calIndexId(entity2));
        IndexEntity entity3 = new IndexEntity();
        entity3.setLayouts(Lists.newArrayList(layout4));
        entity3.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        entity3.setMeasures(Lists.newArrayList(100000, 100002));
        entity3.setId(calIndexId(entity3));

        // for auto-modeling proposition
        Map<LayoutEntity, LayoutEntity> map = IndexPlanReduceUtil
                .collectRedundantLayoutsOfAggIndex(Lists.newArrayList(entity1, entity2, entity3), false);
        Assert.assertEquals(1, map.size());
        Assert.assertTrue(map.containsKey(layout4));
        Assert.assertEquals("[2, 1, 3, 4, 100000, 100001, 100002]", map.get(layout4).getColOrder().toString());

        // for garbage-cleaning
        Map<LayoutEntity, LayoutEntity> map2 = IndexPlanReduceUtil
                .collectRedundantLayoutsOfAggIndex(Lists.newArrayList(entity1, entity2, entity3), true);
        Assert.assertEquals(2, map2.size());
        Assert.assertTrue(map2.containsKey(layout1));
        Assert.assertEquals("[1, 2, 3, 4, 100000, 100001, 100002]", map2.get(layout1).getColOrder().toString());
        Assert.assertTrue(map2.containsKey(layout4));
        Assert.assertEquals("[2, 1, 3, 4, 100000, 100001, 100002]", map2.get(layout4).getColOrder().toString());

    }

    @Test
    public void testCollectRedundantTableIndexLayoutsForGarbageCleaning() {
        initTableIndex();

        // case 1: index1.dimensions contains index2.dimensions,
        //         1) different colOrders with same dimensions will not be reduced;
        //         2) if these layouts have same shardByColumns, they will be reduced;
        //         3) if these layouts don't have same shardByColumns, they will not be reduced
        Map<LayoutEntity, LayoutEntity> map1 = IndexPlanReduceUtil
                .collectRedundantLayoutsOfTableIndex(Lists.newArrayList(tableIndex1, tableIndex2), true);
        Assert.assertEquals(1, map1.size());
        LayoutEntity redundant = map1.keySet().iterator().next();
        Assert.assertEquals("[1, 2, 3]", redundant.getColOrder().toString());
        Assert.assertTrue(redundant.getShardByColumns().isEmpty());
        Assert.assertEquals("[1, 2, 3, 4]", map1.get(redundant).getColOrder().toString());

        // case 2: layouts in index4 only shardByColumns are not the same, they will not be reduced;
        //         layouts in index3 don't contain shardByColumns, they will not be reduced.
        Map<LayoutEntity, LayoutEntity> map2 = IndexPlanReduceUtil
                .collectRedundantLayoutsOfTableIndex(Lists.newArrayList(tableIndex3, tableIndex4), true);
        Assert.assertTrue(map2.isEmpty());
    }

    @Test
    public void testCollectRedundantTableIndexLayouts() {
        LayoutEntity[] layoutArray = new LayoutEntity[6];
        layoutArray[0] = new LayoutEntity();
        layoutArray[0].setId(IndexEntity.TABLE_INDEX_START_ID + 1);
        layoutArray[0].setColOrder(Lists.newArrayList(1, 2, 3, 4));
        layoutArray[0].setAuto(true);
        layoutArray[1] = new LayoutEntity();
        layoutArray[1].setId(IndexEntity.TABLE_INDEX_START_ID + 2);
        layoutArray[1].setColOrder(Lists.newArrayList(1, 3, 2, 4));
        layoutArray[1].setAuto(false);
        layoutArray[1].setInProposing(false);
        layoutArray[2] = new LayoutEntity();
        layoutArray[2].setId(IndexEntity.TABLE_INDEX_START_ID + IndexEntity.INDEX_ID_STEP + 1);
        layoutArray[2].setColOrder(Lists.newArrayList(1, 2, 3, 4, 5));
        layoutArray[2].setAuto(true);
        layoutArray[2].setInProposing(true);
        layoutArray[3] = new LayoutEntity();
        layoutArray[3].setId(IndexEntity.TABLE_INDEX_START_ID + 2 * IndexEntity.INDEX_ID_STEP + 1);
        layoutArray[3].setColOrder(Lists.newArrayList(1, 2, 3));
        layoutArray[3].setAuto(true);
        layoutArray[3].setInProposing(true);
        layoutArray[4] = new LayoutEntity();
        layoutArray[4].setId(IndexEntity.TABLE_INDEX_START_ID + 2 * IndexEntity.INDEX_ID_STEP + 2);
        layoutArray[4].setColOrder(Lists.newArrayList(1, 3, 2));
        layoutArray[4].setAuto(true);
        layoutArray[4].setInProposing(false);
        layoutArray[5] = new LayoutEntity();
        layoutArray[5].setId(IndexEntity.TABLE_INDEX_START_ID + 3 * IndexEntity.INDEX_ID_STEP + 1);
        layoutArray[5].setColOrder(Lists.newArrayList(1, 2, 3, 4, 6));
        layoutArray[5].setAuto(true);
        layoutArray[5].setInProposing(true);

        IndexEntity entity1 = new IndexEntity();
        entity1.setLayouts(Lists.newArrayList(layoutArray[0], layoutArray[1]));
        entity1.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        entity1.setId(calIndexId(entity1));
        IndexEntity entity2 = new IndexEntity();
        entity2.setLayouts(Lists.newArrayList(layoutArray[2]));
        entity2.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5));
        entity2.setId(calIndexId(entity2));
        IndexEntity entity3 = new IndexEntity();
        entity3.setLayouts(Lists.newArrayList(layoutArray[3], layoutArray[4]));
        entity3.setDimensions(Lists.newArrayList(1, 2, 3));
        entity3.setId(calIndexId(entity3));
        IndexEntity entity4 = new IndexEntity();
        entity4.setLayouts(Lists.newArrayList(layoutArray[5]));
        entity4.setDimensions(Lists.newArrayList(1, 2, 3, 4, 6));
        entity4.setId(calIndexId(entity4));

        // for auto-modeling proposition
        Map<LayoutEntity, LayoutEntity> map1 = IndexPlanReduceUtil
                .collectRedundantLayoutsOfTableIndex(Lists.newArrayList(entity1, entity2, entity3, entity4), false);
        Assert.assertEquals(1, map1.size());
        Assert.assertTrue(map1.containsKey(layoutArray[3]));
        Assert.assertEquals("[1, 2, 3, 4, 5]", map1.get(layoutArray[3]).getColOrder().toString());

        // for garbage-cleaning
        Map<LayoutEntity, LayoutEntity> map2 = IndexPlanReduceUtil
                .collectRedundantLayoutsOfTableIndex(Lists.newArrayList(entity1, entity2, entity3, entity4), true);
        Assert.assertEquals(3, map2.size());
        Assert.assertTrue(map2.containsKey(layoutArray[3]));
        Assert.assertEquals("[1, 2, 3, 4, 5]", map2.get(layoutArray[3]).getColOrder().toString());
        Assert.assertTrue(map2.containsKey(layoutArray[0]));
        Assert.assertEquals("[1, 2, 3, 4, 5]", map2.get(layoutArray[0]).getColOrder().toString());
        Assert.assertTrue(map2.containsKey(layoutArray[4]));
        Assert.assertEquals("[1, 3, 2, 4]", map2.get(layoutArray[4]).getColOrder().toString());

    }

    private void initTableIndex() {
        /*
         * - Table index: index1(dims[1, 2, 3, 4]
         * -                |----layout([1, 2, 3, 4])
         * -                |----layout([1, 3, 2, 4])
         * -              index2(dims[1, 2, 3])
         * -                |----layout([1, 2, 3])
         * -                |----layout([1, 2, 3], shardBy=2)
         * -              index3(dims[4])
         * -                |----layout([4])
         * -              index4(dims([4, 5])
         * -                |----layout([4, 5], shardBy=4)
         * -                |----layout([4, 5], shardBy=5)
         */
        LayoutEntity layout1, layout2, layout3, layout4, layout5, layout6, layout7;
        layout1 = new LayoutEntity();
        layout1.setId(IndexEntity.TABLE_INDEX_START_ID + 1);
        layout1.setColOrder(Lists.newArrayList(1, 2, 3, 4));
        layout1.setAuto(true);
        layout2 = new LayoutEntity();
        layout2.setId(IndexEntity.TABLE_INDEX_START_ID + 2);
        layout2.setColOrder(Lists.newArrayList(1, 3, 2, 4));
        layout2.setAuto(true);

        layout3 = new LayoutEntity();
        layout3.setId(IndexEntity.INDEX_ID_STEP + IndexEntity.TABLE_INDEX_START_ID + 1);
        layout3.setColOrder(Lists.newArrayList(1, 2, 3));
        layout3.setAuto(true);
        layout4 = new LayoutEntity();
        layout4.setId(IndexEntity.INDEX_ID_STEP + IndexEntity.TABLE_INDEX_START_ID + 2);
        layout4.setColOrder(Lists.newArrayList(1, 2, 3));
        layout4.setShardByColumns(Lists.newArrayList(2));
        layout4.setAuto(true);

        layout5 = new LayoutEntity();
        layout5.setId(2 * IndexEntity.INDEX_ID_STEP + IndexEntity.TABLE_INDEX_START_ID + 1);
        layout5.setColOrder(Lists.newArrayList(4));
        layout5.setAuto(true);

        layout6 = new LayoutEntity();
        layout6.setId(3 * IndexEntity.INDEX_ID_STEP + IndexEntity.TABLE_INDEX_START_ID + 1);
        layout6.setColOrder(Lists.newArrayList(4, 5));
        layout6.setShardByColumns(Lists.newArrayList(4));
        layout6.setAuto(true);
        layout7 = new LayoutEntity();
        layout7.setId(3 * IndexEntity.INDEX_ID_STEP + IndexEntity.TABLE_INDEX_START_ID + 2);
        layout7.setColOrder(Lists.newArrayList(4, 5));
        layout7.setShardByColumns(Lists.newArrayList(5));
        layout7.setAuto(true);

        tableIndex1 = new IndexEntity();
        tableIndex1.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        tableIndex1.setLayouts(Lists.newArrayList(layout1, layout2));
        tableIndex1.setId(calIndexId(tableIndex1));

        tableIndex2 = new IndexEntity();
        tableIndex2.setDimensions(Lists.newArrayList(1, 2, 3));
        tableIndex2.setLayouts(Lists.newArrayList(layout3, layout4));
        tableIndex2.setId(calIndexId(tableIndex2));

        tableIndex3 = new IndexEntity();
        tableIndex3.setDimensions(Lists.newArrayList(4));
        tableIndex3.setLayouts(Lists.newArrayList(layout5));
        tableIndex3.setId(calIndexId(tableIndex3));

        tableIndex4 = new IndexEntity();
        tableIndex4.setDimensions(Lists.newArrayList(4, 5));
        tableIndex4.setLayouts(Lists.newArrayList(layout6, layout7));
        tableIndex4.setId(calIndexId(tableIndex4));
    }

    private void initAggIndex() {
        /*
         * - Agg index: index1(dims[1, 2, 3, 4], measures[100000, 100003])
         * -              |----layout([1, 2, 3, 4, 100000, 100003])
         * -              |----layout([2, 1, 3, 4, 100000, 100003])
         * -            index2
         * -              |----layout([1, 2, 3, 4, 100000, 100002, 100003])
         * -            index3
         * -              |----layout([1, 2, 3, 4, 100000, 100001])
         * -            index4
         * -              |----layout([1, 2, 3, 4, 5, 100000, 100001])
         * -            index5
         * -              |----layout([100000, 100001])
         * -            index6
         * -              |----layout([100000])
         */
        LayoutEntity layout1, layout2, layout3, layout4, layout5, layout6, layout7;
        layout1 = new LayoutEntity();
        layout1.setId(1);
        layout1.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100003));
        layout2 = new LayoutEntity();
        layout2.setId(2);
        layout2.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000, 100003));
        layout3 = new LayoutEntity();
        layout3.setId(10001);
        layout3.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100001));
        layout4 = new LayoutEntity();
        layout4.setId(20001);
        layout4.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100002, 100003));
        layout5 = new LayoutEntity();
        layout5.setId(30001);
        layout5.setColOrder(Lists.newArrayList(1, 2, 3, 4, 5, 100000, 100001));
        layout6 = new LayoutEntity();
        layout6.setId(40001);
        layout6.setColOrder(Lists.newArrayList(100000, 100001));
        layout7 = new LayoutEntity();
        layout7.setId(50001);
        layout7.setColOrder(Lists.newArrayList(100000));

        aggIndex1 = new IndexEntity();
        aggIndex1.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        aggIndex1.setMeasures(Lists.newArrayList(100000, 100003));
        aggIndex1.setLayouts(Lists.newArrayList(layout1, layout2));
        aggIndex1.setId(calIndexId(aggIndex1));

        aggIndex2 = new IndexEntity();
        aggIndex2.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        aggIndex2.setMeasures(Lists.newArrayList(100000, 100002, 100003));
        aggIndex2.setLayouts(Lists.newArrayList(layout4));
        aggIndex2.setId(calIndexId(aggIndex2));

        aggIndex3 = new IndexEntity();
        aggIndex3.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        aggIndex3.setMeasures(Lists.newArrayList(100000, 100001));
        aggIndex3.setLayouts(Lists.newArrayList(layout3));
        aggIndex3.setId(calIndexId(aggIndex3));

        aggIndex4 = new IndexEntity();
        aggIndex4.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5));
        aggIndex4.setMeasures(Lists.newArrayList(100000, 100001));
        aggIndex4.setLayouts(Lists.newArrayList(layout5));
        aggIndex4.setId(calIndexId(aggIndex4));

        aggIndex5 = new IndexEntity();
        aggIndex5.setDimensions(Lists.newArrayList());
        aggIndex5.setMeasures(Lists.newArrayList(100000, 100001));
        aggIndex5.setLayouts(Lists.newArrayList(layout6));
        aggIndex5.setId(calIndexId(aggIndex5));

        aggIndex6 = new IndexEntity();
        aggIndex6.setDimensions(Lists.newArrayList());
        aggIndex6.setMeasures(Lists.newArrayList(100000, 100001));
        aggIndex6.setLayouts(Lists.newArrayList(layout7));
        aggIndex6.setId(calIndexId(aggIndex6));
    }

    private long calIndexId(IndexEntity entity) {
        if (entity.getLayouts().isEmpty()) {
            throw new IllegalStateException("Index without layouts!");
        }
        return entity.getLayouts().get(0).getId() - entity.getLayouts().get(0).getId() % IndexEntity.INDEX_ID_STEP;
    }
}
