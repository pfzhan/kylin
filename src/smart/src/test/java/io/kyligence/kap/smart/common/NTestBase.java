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

package io.kyligence.kap.smart.common;

import java.io.File;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Before;

import com.google.common.io.Files;

import io.kyligence.kap.smart.query.Utils;

public abstract class NTestBase {
    protected String metaDir = "src/test/resources/nsmart/learn_kylin/meta";
    protected String proj = "learn_kylin";
    protected File tmpMeta;
    protected KylinConfig kylinConfig;

    @Before
    public void setUp() throws Exception {
        tmpMeta = Files.createTempDir();
        FileUtils.copyDirectory(new File(metaDir), tmpMeta);

        kylinConfig = Utils.smartKylinConfig(tmpMeta.getAbsolutePath());
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);
    }

    @After
    public void tearDown() throws Exception {
        KylinConfig.removeKylinConfigThreadLocal();
        if (tmpMeta != null)
            FileUtils.forceDelete(tmpMeta);
        ResourceStore.clearCache(kylinConfig);
    }

    protected <T> int countInnerObj(Collection<T>... list) {
        int i = 0;
        for (Collection<T> l : list) {
            i += l.size();
        }
        return i;
    }
}
