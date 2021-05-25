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

package io.kyligence.kap.clickhouse.job;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;

@Slf4j
public class AzureBlobClient {

    private final CloudBlobClient cloudBlobClient;
    private final BlobUrl blobUrl;

    public AzureBlobClient(CloudBlobClient cloudBlobClient, BlobUrl blobUrl) {
        this.cloudBlobClient = cloudBlobClient;
        this.blobUrl = blobUrl;
    }

    public static AzureBlobClient getInstance() {
        return Singletons.getInstance(AzureBlobClient.class, clientClass -> {
            String workDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
            BlobUrl blobUrl = BlobUrl.fromBlobUrl(workDir);
            URI endpoint = URI.create(blobUrl.getHttpEndpoint());
            StorageCredentials credentials = null;
            try {
                credentials = StorageCredentials.tryParseCredentials(blobUrl.getConnectionString());
            } catch (Exception e) {
                log.error("Blob connection string for working dir {} is error", workDir, e);
                ExceptionUtils.rethrow(e);
            }
            CloudBlobClient blobClient;
            if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
                blobClient = CloudStorageAccount.getDevelopmentStorageAccount().createCloudBlobClient();
            } else {
                blobClient = new CloudBlobClient(endpoint, credentials);
            }
            return new AzureBlobClient(blobClient, blobUrl);
        });
    }

    public String generateSasKey(String blobPath, int expireHours) {
        try {
            CloudBlobContainer container = this.cloudBlobClient.getContainerReference(blobUrl.getContainer());
            CloudBlob cloudBlob = getBlob(container, URI.create(blobPath).getPath());
            SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
            policy.setPermissionsFromString("r");
            policy.setSharedAccessStartTime(Date.from(OffsetDateTime.now(ZoneId.of("UTC")).minusHours(expireHours).toInstant()));
            policy.setSharedAccessExpiryTime(Date.from(OffsetDateTime.now(ZoneId.of("UTC")).plusHours(expireHours).toInstant()));
            return cloudBlob.generateSharedAccessSignature(policy, null);
        } catch (URISyntaxException | StorageException | InvalidKeyException e) {
            log.error("generate SAS key for {} failed", blobPath, e);
            return ExceptionUtils.rethrow(e);
        }
    }

    public CloudBlob getBlob(CloudBlobContainer container, String path) {
        // blob name can't start with /
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        val containerClass = container.getClass();
        Method getBlobMethod = null;
        try {
            getBlobMethod = containerClass.getMethod("getBlobReferenceFromServer", String.class);
        } catch (NoSuchMethodException e) {
            // support hadoop-azure:3.2.0
        }
        if (getBlobMethod == null) {
            try {
                // support hadoop-azure:2.7.x
                getBlobMethod = containerClass.getMethod("getBlockBlobReference", String.class);
            } catch (NoSuchMethodException e) {
                log.error("Only support hadoop-azure 2.7.x, 3.2.x and 3.3.x, please check hadoop-azure version!", e);
                ExceptionUtils.rethrow(e);
            }
        }
        try {
            return (CloudBlob) getBlobMethod.invoke(container, path);
        } catch (IllegalAccessException | InvocationTargetException e) {
            log.error("Can't access method {}", getBlobMethod.getName(), e);
            return ExceptionUtils.rethrow(e);
        }
    }


}
