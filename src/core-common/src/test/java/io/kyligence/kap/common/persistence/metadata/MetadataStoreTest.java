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
package io.kyligence.kap.common.persistence.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class MetadataStoreTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        staticCreateTestMetadata();
    }

    @After
    public void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testVerify() throws Exception {
        //copy an metadata image to junit folder
        val junitFolder = temporaryFolder.getRoot();
        ResourceTool.copy(getTestConfig(), KylinConfig.createInstanceFromUri(junitFolder.getAbsolutePath()),
                "/default");

        getTestConfig().setMetadataUrl(junitFolder.getAbsolutePath());
        val metadataStore = ResourceStore.createMetadataStore(getTestConfig(), MetadataStore.METADATA_NAMESPACE);

        //add illegal file,the verify result is not qualified
        Paths.get(junitFolder.getAbsolutePath(), MetadataStore.METADATA_NAMESPACE, "/IllegalFile").toFile()
                .createNewFile();
        val verifyResultWithIllegalFile = metadataStore.verify();
        Assertions.assertThat(verifyResultWithIllegalFile.illegalFiles).hasSize(1).contains("/IllegalFile");
        assertFalse(verifyResultWithIllegalFile.isQualified());
        Paths.get(junitFolder.getAbsolutePath(), MetadataStore.METADATA_NAMESPACE, "/IllegalFile").toFile().delete();

        //add illegal project dir ,the verify result is not qualified
        Paths.get(junitFolder.getAbsolutePath(), MetadataStore.METADATA_NAMESPACE, "/IllegalProject").toFile().mkdir();
        Paths.get(junitFolder.getAbsolutePath(), MetadataStore.METADATA_NAMESPACE, "/IllegalProject/test.json").toFile()
                .createNewFile();
        val verifyResultWithIllegalProject = metadataStore.verify();
        Assertions.assertThat(verifyResultWithIllegalProject.illegalProjects).hasSize(1).contains("IllegalProject");
        Assertions.assertThat(verifyResultWithIllegalProject.illegalFiles).hasSize(1)
                .contains("/IllegalProject/test.json");
        assertFalse(verifyResultWithIllegalProject.isQualified());
        Paths.get(junitFolder.getAbsolutePath(), MetadataStore.METADATA_NAMESPACE, "/IllegalProject/test.json").toFile()
                .delete();
        Paths.get(junitFolder.getAbsolutePath(), MetadataStore.METADATA_NAMESPACE, "/IllegalProject").toFile().delete();

        //add legal project and file,the verify result is qualified
        Paths.get(junitFolder.getAbsolutePath(), MetadataStore.METADATA_NAMESPACE, "/IllegalProject").toFile().mkdir();
        Paths.get(junitFolder.getAbsolutePath(), MetadataStore.METADATA_NAMESPACE, "/IllegalProject/project.json")
                .toFile().createNewFile();
        val verifyResultWithLegalProject = metadataStore.verify();
        Assertions.assertThat(verifyResultWithLegalProject.illegalFiles).isEmpty();
        Assertions.assertThat(verifyResultWithLegalProject.illegalProjects).isEmpty();
        assertTrue(verifyResultWithLegalProject.isQualified());

        //the metadata dir doesn't have uuid file
        assertFalse(metadataStore.verify().existUUIDFile);
        Paths.get(junitFolder.getAbsolutePath(), MetadataStore.METADATA_NAMESPACE, "/UUID").toFile().createNewFile();
        assertTrue(metadataStore.verify().existUUIDFile);

        //the metadata dir doesn't have user group file
        assertFalse(metadataStore.verify().existUserGroupFile);
        Paths.get(junitFolder.getAbsolutePath(), MetadataStore.METADATA_NAMESPACE, "/user_group").toFile()
                .createNewFile();
        assertTrue(metadataStore.verify().existUserGroupFile);

        //the metadata dir doesn't have user dir
        assertFalse(metadataStore.verify().existUserDir);
        Paths.get(junitFolder.getAbsolutePath(), MetadataStore.METADATA_NAMESPACE, "/user").toFile().mkdir();
        Paths.get(junitFolder.getAbsolutePath(), MetadataStore.METADATA_NAMESPACE, "/user/ADMIN").toFile()
                .createNewFile();
        assertTrue(metadataStore.verify().existUserDir);
    }
}
