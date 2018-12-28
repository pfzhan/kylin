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

package io.kyligence.kap.cube.model;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class NDataSegDetailsManagerTest extends NLocalFileMetadataTestCase {
    private String projectDefault = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasic() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager dsMgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataSegDetailsManager mgr = NDataSegDetailsManager.getInstance(testConfig, projectDefault);

        NDataflow df = dsMgr.getDataflow("ncube_basic");
        NDataSegment segment = df.getLastSegment();
        Assert.assertNotNull(segment);

        NDataSegDetails details = mgr.getForSegment(segment);
        Assert.assertNotNull(details);
        Assert.assertEquals(segment.getId(), details.getUuid());
        Assert.assertEquals(8, details.getCuboids().size());
        Assert.assertSame(segment.getConfig().base(), details.getConfig().base());

        details = mgr.upsertForSegment(details);
        details = mgr.getForSegment(segment);
        Assert.assertNotNull(details);

        mgr.removeForSegment(details.getDataflow(), details.getUuid());
        Assert.assertNull(mgr.getForSegment(segment));
    }
}
