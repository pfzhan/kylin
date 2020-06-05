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

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;

public class SecretKeyUtilTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Test
    public void testEncryptAndDecryptToken() throws Exception {
        String originToken = "12345_1583840099000";

        SecretKey secretKey = SecretKeyUtil.generateNewAESKey();
        byte[] encryptedToken = SecretKeyUtil.encryptToken(secretKey, originToken);

        String decryptedToken = SecretKeyUtil.decryptToken(secretKey, encryptedToken);
        Assert.assertEquals(originToken, decryptedToken);

        // create key from bytes
        byte[] keyBytes = secretKey.getEncoded();
        SecretKeySpec secretKeySpec = new SecretKeySpec(keyBytes, "AES");
        String decryptedToken2 = SecretKeyUtil.decryptToken(secretKeySpec, encryptedToken);
        Assert.assertEquals(originToken, decryptedToken2);
    }

    @Test
    public void testEncryptAndDecryptByStrKey() throws Exception {

        String originToken = "12345_1583840099000";

        String key = "kylin_metadata";

        byte[] encryptedToken = SecretKeyUtil.encryptToken(key, originToken);

        String decryptedToken = SecretKeyUtil.decryptToken(key, encryptedToken);

        Assert.assertEquals(originToken, decryptedToken);
    }

    @Test
    public void testInitKGSecretKey() throws Exception {
        final String mainFolder = temporaryFolder.getRoot().getAbsolutePath() + "/" + testName.getMethodName();
        FileUtils.forceMkdir(new File(mainFolder));

        String sourceValue = System.getenv("KYLIN_HOME");

        System.setProperty("KYLIN_HOME", mainFolder);

        SecretKeyUtil.initKGSecretKey();

        Assert.assertEquals(16, FileUtils
                .readFileToByteArray(new File(mainFolder + '/' + SecretKeyUtil.KG_SECRET_KEY_FILE_NAME)).length);

        SecretKey secretKey = SecretKeyUtil.readKGSecretKeyFromFile();
        Assert.assertEquals(16, secretKey.getEncoded().length);

        if (null == sourceValue) {
            System.clearProperty("KYLIN_HOME");
        } else {
            System.setProperty("KYLIN_HOME", sourceValue);
        }
        Assert.assertEquals(sourceValue, System.getenv("KYLIN_HOME"));
    }
}
