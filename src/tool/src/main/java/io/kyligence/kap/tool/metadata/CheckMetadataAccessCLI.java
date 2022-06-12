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

package io.kyligence.kap.tool.metadata;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.project.NProjectManager;

public class CheckMetadataAccessCLI {
    protected static final Logger logger = LoggerFactory.getLogger(CheckMetadataAccessCLI.class);

    public boolean testAccessMetadata() {

        String projectName = RandomUtil.randomUUIDStr();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore store = ResourceStore.getKylinMetaStore(config);

        logger.info("Start to test. Test metastore is: " + config.getMetadataUrl().toString());
        //test store's connection.
        try {
            store.collectResourceRecursively(ResourceStore.PROJECT_ROOT, MetadataConstants.FILE_SURFIX);
        } catch (Exception e) {
            logger.error("Connection test failed." + e.getCause());
            return false;
        }

        //Test CURD

        //test create.
        try {
            UnitOfWork.doInTransactionWithRetry(() -> {
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).createProject(projectName, "test",
                        "This is a test project", null);
                return null;
            }, UnitOfWork.GLOBAL_UNIT);
        } catch (TransactionException e) {
            logger.error("Creation test failed." + e.getMessage());
            return false;
        }

        NProjectManager projectManager = NProjectManager.getInstance(config);
        if (projectManager.getProject(projectName) == null) {
            logger.error("Creation test failed.");
            return false;
        }

        //test update.
        try {
            UnitOfWork.doInTransactionWithRetry(() -> {
                ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                        .getProject(projectName);
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).updateProject(projectInstance,
                        projectName, "Still a test project", null);
                return null;
            }, projectName);
        } catch (TransactionException e) {
            logger.error("Update test failed." + e.getMessage());
            clean(store, ProjectInstance.concatResourcePath(projectName));
            return false;
        }

        //test delete
        try {
            UnitOfWork.doInTransactionWithRetry(() -> {
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).forceDropProject(projectName);
                return null;
            }, projectName);
        } catch (TransactionException e) {
            clean(store, ProjectInstance.concatResourcePath(projectName));
            logger.error("Deletion test failed");
            return false;
        }

        return true;
    }

    private void clean(ResourceStore store, String path) {
        try {
            UnitOfWork.doInTransactionWithRetry(() -> {
                store.deleteResource(path);
                return null;
            }, path.split("/")[0]);
        } catch (TransactionException e) {
            throw new RuntimeException("Failed to cleanup test metadata, it will remain in the resource store: "
                    + store.getConfig().getMetadataUrl(), e);
        }
    }

    public static void main(String[] args) {
        StorageURL metadataUrl = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        if (metadataUrl.metadataLengthIllegal()) {
            logger.info("the maximum length of metadata_name allowed is {}", StorageURL.METADATA_MAX_LENGTH);
            Unsafe.systemExit(1);
        }
        CheckMetadataAccessCLI cli = new CheckMetadataAccessCLI();

        if (args.length != 1) {
            logger.info("Usage: CheckMetadataAccessCLI <repetition>");
            Unsafe.systemExit(1);
        }

        long repetition = Long.parseLong(args[0]);

        while (repetition > 0) {
            if (!cli.testAccessMetadata()) {
                logger.error("Test failed.");
                Unsafe.systemExit(1);
            }
            repetition--;
        }

        logger.info("Test succeed.");
        Unsafe.systemExit(0);
    }
}
