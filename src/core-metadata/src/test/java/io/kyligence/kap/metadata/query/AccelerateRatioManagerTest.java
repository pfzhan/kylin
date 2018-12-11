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

package io.kyligence.kap.metadata.query;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AccelerateRatioManagerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testBasics() {
        AccelerateRatioManager accelerateRatioManager = AccelerateRatioManager.getInstance(getTestConfig(), PROJECT);
        // get
        AccelerateRatio ratio = accelerateRatioManager.get();
        Assert.assertNull(ratio);

        // save
        accelerateRatioManager.increment(100, 1000);
        ratio = accelerateRatioManager.get();
        Assert.assertNotNull(ratio);
        Assert.assertEquals(100, ratio.getQueryNumOfMarkedAsFavorite());
        Assert.assertEquals(1000, ratio.getOverallQueryNum());
        
        accelerateRatioManager.increment(100, 3000);
        ratio = accelerateRatioManager.get();
        Assert.assertEquals(200, ratio.getQueryNumOfMarkedAsFavorite());
        Assert.assertEquals(4000, ratio.getOverallQueryNum());
    }
}
