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
package io.kyligence.kap.common.util;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.persistence.ResourceStore;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.val;

public class MetadataChecker {

    private MetadataStore metadataStore;

    private static final String JSON_SUFFIX = ".json";

    public MetadataChecker(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Getter(AccessLevel.PUBLIC)
    public class VerifyResult {
        boolean existUUIDFile = false;
        boolean existImageFile = false;
        boolean existACLDir = false;
        boolean existUserDir = false;
        boolean existUserGroupFile = false;
        boolean existCompressedFile = false;
        boolean existModelDescFile = false;
        boolean existIndexPlanFile = false;
        boolean existTable = false;
        boolean existVersionFile = false;
        private Set<String> illegalProjects = Sets.newHashSet();
        private Set<String> illegalFiles = Sets.newHashSet();

        public boolean isQualified() {
            return illegalProjects.isEmpty() && illegalFiles.isEmpty();
        }

        public boolean isModelMetadataQualified() {
            return illegalFiles.isEmpty() && existUUIDFile && existModelDescFile && existIndexPlanFile && existTable;
        }

        public String getResultMessage() {
            StringBuilder resultMessage = new StringBuilder();

            resultMessage.append("the uuid file exists : " + existUUIDFile + "\n");
            resultMessage.append("the image file exists : " + existImageFile + "\n");
            resultMessage.append("the user_group file exists : " + existUserGroupFile + "\n");
            resultMessage.append("the user dir exist : " + existUserDir + "\n");
            resultMessage.append("the acl dir exist : " + existACLDir + "\n");

            if (!illegalProjects.isEmpty()) {
                resultMessage.append("illegal projects : \n");
                for (String illegalProject : illegalProjects) {
                    resultMessage.append("\t" + illegalProject + "\n");
                }
            }

            if (!illegalFiles.isEmpty()) {
                resultMessage.append("illegal files : \n");
                for (String illegalFile : illegalFiles) {
                    resultMessage.append("\t" + illegalFile + "\n");
                }
            }

            return resultMessage.toString();
        }

    }

    public VerifyResult verify() {
        VerifyResult verifyResult = new VerifyResult();

        // The valid metadata image contains at least the following conditions：
        //     1.may have one UUID file
        //     2.may have one _global dir which may have one user_group file or one user dir or one acl dir
        //     3.all other subdir as a project and must have only one project.json file

        val allFiles = metadataStore.list(File.separator);
        for (final String file : allFiles) {
            //check uuid file
            if (file.equals(ResourceStore.METASTORE_UUID_TAG)) {
                verifyResult.existUUIDFile = true;
                continue;
            }

            //check VERSION file
            if (file.equals(ResourceStore.VERSION_FILE)) {
                verifyResult.existVersionFile = true;
                continue;
            }

            //check user_group file
            if (file.equals(ResourceStore.USER_GROUP_ROOT)) {
                verifyResult.existUserGroupFile = true;
                continue;
            }

            //check user dir
            if (file.startsWith(ResourceStore.USER_ROOT)) {
                verifyResult.existUserDir = true;
                continue;
            }

            //check acl dir
            if (file.startsWith(ResourceStore.ACL_ROOT)) {
                verifyResult.existACLDir = true;
                continue;
            }

            if (file.startsWith(ResourceStore.METASTORE_IMAGE)) {
                verifyResult.existImageFile = true;
                continue;
            }

            if (file.startsWith(ResourceStore.COMPRESSED_FILE)) {
                verifyResult.existCompressedFile = true;
                continue;
            }

            //check illegal file which locates in metadata dir
            if (File.separator.equals(Paths.get(file).toFile().getParent())) {
                verifyResult.illegalFiles.add(file);
                continue;
            }

            //check project dir
            final String project = Paths.get(file).getName(0).toString();
            if (Paths.get(ResourceStore.GLOBAL_PROJECT).getName(0).toString().equals(project)) {
                continue;
            }
            if (!allFiles
                    .contains(Paths.get(File.separator + "_global", "project", project + JSON_SUFFIX).toString())) {
                verifyResult.illegalProjects.add(project);
                verifyResult.illegalFiles.add(file);
            }
        }

        return verifyResult;

    }

    public VerifyResult verifyModelMetadata(List<String> resourcePath) {
        VerifyResult verifyResult = new VerifyResult();

        for (final String resoucePath : resourcePath) {
            // check uuid file
            if (resoucePath.equals(ResourceStore.METASTORE_UUID_TAG)) {
                verifyResult.existUUIDFile = true;
                continue;
            }

            //check VERSION file
            if (resoucePath.equals(ResourceStore.VERSION_FILE)) {
                verifyResult.existVersionFile = true;
                continue;
            }

            // start with project name except uuid file
            if (resoucePath.startsWith(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT)
                    || resoucePath.startsWith(ResourceStore.INDEX_PLAN_RESOURCE_ROOT)
                    || resoucePath.startsWith(ResourceStore.TABLE_RESOURCE_ROOT)
                    || !resoucePath.endsWith(JSON_SUFFIX)) {
                verifyResult.illegalFiles.add(resoucePath);
            }

            // check model desc
            if (resoucePath.contains(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT)
                    && resoucePath.endsWith(JSON_SUFFIX)) {
                verifyResult.existModelDescFile = true;
                continue;
            }

            // check index plan
            if (resoucePath.contains(ResourceStore.INDEX_PLAN_RESOURCE_ROOT) && resoucePath.endsWith(JSON_SUFFIX)) {
                verifyResult.existIndexPlanFile = true;
                continue;
            }

            // check table
            if (resoucePath.contains(ResourceStore.TABLE_RESOURCE_ROOT) && resoucePath.endsWith(JSON_SUFFIX)) {
                verifyResult.existTable = true;
                continue;
            }

            //check illegal file which locates in metadata dir
            if (File.separator.equals(Paths.get(resoucePath).toFile().getParent())) {
                verifyResult.illegalFiles.add(resoucePath);
            }
        }

        return verifyResult;
    }
}
