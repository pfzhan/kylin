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
package io.kyligence.kap.secondstorage.test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.util.Locale;

public class EnableLocalMeta extends ExternalResource implements KapTest {

    private final String[] overlay;
    protected final String project;
    private final NLocalFileMetadataTestCase internalMeta = new NLocalFileMetadataTestCase();

    public EnableLocalMeta(String project, String... extraMeta) {
        this.project = project;
        this.overlay = extraMeta;
    }

    public KylinConfig getTestConfig() {
        return NLocalFileMetadataTestCase.getTestConfig();
    }

    @Override
    protected void before() throws Throwable {
        internalMeta.createTestMetadata(overlay);
        final File tempMetadataDirectory = NLocalFileMetadataTestCase.getTempMetadataDirectory();
        Assert.assertNotNull(tempMetadataDirectory);
        Assert.assertTrue(tempMetadataDirectory.exists());
        final File testProjectMetaDir = new File(tempMetadataDirectory.getPath() + "/metadata/" + project);
        final String message = String.format(Locale.ROOT, "%s's meta (%s) doesn't exist, please check!",
                project, testProjectMetaDir.getCanonicalPath());
        Assert.assertTrue(message, testProjectMetaDir.exists());
    }

    @Override
    protected void after() {
        internalMeta.cleanupTestMetadata();
        internalMeta.restoreSystemProps();
    }

    @Override
    public void overwriteSystemProp(String key, String value){
        internalMeta.overwriteSystemProp(key, value);
    }
}
