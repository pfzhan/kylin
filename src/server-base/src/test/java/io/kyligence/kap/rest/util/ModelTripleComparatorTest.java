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
package io.kyligence.kap.rest.util;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.val;

public class ModelTripleComparatorTest extends NLocalFileMetadataTestCase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testModelTripleCompare() {
        val comparator = new ModelTripleComparator("queryHitCount", true, 1);
        val modelTripleOfNullValue = new ModelTriple(null, null);
        Assert.assertEquals(0, comparator.compare(null, modelTripleOfNullValue));
        Assert.assertEquals(0, comparator.compare(modelTripleOfNullValue, null));
        Assert.assertEquals(0, comparator.compare(null, null));
        Assert.assertEquals(0, comparator.compare(modelTripleOfNullValue, modelTripleOfNullValue));

        val dataflow1 = new NDataflow();
        val modelTripleOfNullDataModel1 = new ModelTriple(dataflow1, null);
        val modelTripleOfNullDataModel2 = new ModelTriple(new NDataflow(), null);

        dataflow1.setQueryHitCount(10);
        Assert.assertEquals(1, comparator.compare(modelTripleOfNullDataModel1, modelTripleOfNullDataModel2));

        val comparator1 = new ModelTripleComparator("lastModified", false, 2);
        val dataModel1 = new NDataModel();
        val dataModel2 = new NDataModel();
        val modelTripleOfNullDataflow1 = new ModelTriple(null, dataModel1);
        val modelTripleOfNullDataflow2 = new ModelTriple(null, dataModel2);
        Assert.assertEquals(0, comparator1.compare(modelTripleOfNullDataflow1, modelTripleOfNullDataflow2));

        val comparator2 = new ModelTripleComparator("calcObject", true, 3);
        modelTripleOfNullDataflow1.setCalcObject("t1");
        Assert.assertEquals(-1, comparator2.compare(modelTripleOfNullDataflow1, modelTripleOfNullDataflow2));
    }

    @Test
    public void testGetPropertyValueException() {
        val comparator = new ModelTripleComparator("usage", true, 1);
        thrown.expect(Exception.class);
        comparator.getPropertyValue(null);
    }
}
