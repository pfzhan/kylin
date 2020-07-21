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

import com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.common.KylinConfig;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class SecretKeyUtil {
    private static final String STRING_ENCODE = "UTF-8";

    private static final String ENCRYPTION_ALGORITHM_AES = "AES";

    private static final String ENCRYPTION_ALGORITHM_MD5 = "MD5";

    @VisibleForTesting
    public static final String KG_SECRET_KEY_FILE_NAME = "kg_secret_key";

    private static SecretKey kgSecretKey = null;

    public static byte[] encryptToken(SecretKey secretKey, String token) throws Exception {
        Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM_AES);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        byte[] byteContent = token.getBytes(STRING_ENCODE);
        return cipher.doFinal(byteContent);
    }

    public static String decryptToken(SecretKey secretKey, byte[] encryptedToken) throws Exception {
        Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM_AES);
        cipher.init(Cipher.DECRYPT_MODE, secretKey);
        byte[] result = cipher.doFinal(encryptedToken);
        return new String(result, STRING_ENCODE);
    }

    public static byte[] encryptToken(String key, String token) throws Exception {
        MessageDigest md = MessageDigest.getInstance(ENCRYPTION_ALGORITHM_MD5);
        byte[] digest = md.digest(key.getBytes(STRING_ENCODE));
        SecretKeySpec keySpec = new SecretKeySpec(digest, ENCRYPTION_ALGORITHM_AES);
        return encryptToken(keySpec, token);
    }

    public static String decryptToken(String key, byte[] encryptedToken) throws Exception {
        MessageDigest md = MessageDigest.getInstance(ENCRYPTION_ALGORITHM_MD5);
        byte[] digest = md.digest(key.getBytes(STRING_ENCODE));
        SecretKeySpec keySpec = new SecretKeySpec(digest, ENCRYPTION_ALGORITHM_AES);
        return decryptToken(keySpec, encryptedToken);
    }

    /**
     * for KG
     * @throws IOException
     */
    public static void initKGSecretKey() throws IOException, NoSuchAlgorithmException {
        File kgSecretKeyFile = new File(KylinConfig.getKylinHome(), KG_SECRET_KEY_FILE_NAME);
        if (kgSecretKeyFile.exists()) {
            Files.delete(kgSecretKeyFile.toPath());
        }

        if (null == kgSecretKey) {
            kgSecretKey = generateNewAESKey();
        }
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(kgSecretKeyFile))) {
            bos.write(kgSecretKey.getEncoded());
        }
    }

    /**
     * for KG
     * @throws IOException
     */
    public static void checkKGSecretKeyFile() throws IOException, NoSuchAlgorithmException {
        File kgSecretKeyFile = new File(KylinConfig.getKylinHome(), KG_SECRET_KEY_FILE_NAME);
        if (kgSecretKeyFile.exists()) {
            return;
        }

        initKGSecretKey();
    }

    /**
     * for KG
     * get cached kgSecretKey
     * @return SecretKey
     */
    public static SecretKey getKGSecretKey() {
        return kgSecretKey;
    }

    /**
     * for KG
     * read KgSecretKey from file
     * @return SecretKey
     * @throws IOException
     */
    public static SecretKey readKGSecretKeyFromFile() throws IOException {
        File kgSecretKeyFile = new File(KylinConfig.getKylinHome(), KG_SECRET_KEY_FILE_NAME);
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(kgSecretKeyFile))) {
            byte[] keyBytes = new byte[16];
            if (bis.read(keyBytes) < 1) {
                throw new RuntimeException(String.format("%s file is empty!", KG_SECRET_KEY_FILE_NAME));
            }
            return new SecretKeySpec(keyBytes, ENCRYPTION_ALGORITHM_AES);
        }
    }

    /**
     * for KG
     * @return
     */
    public static SecretKey generateNewAESKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance(ENCRYPTION_ALGORITHM_AES);
        keyGenerator.init(128, new SecureRandom());
        return keyGenerator.generateKey();
    }

    /**
     * for KG
     * @param kgSecretKey
     * @param kylinPid
     * @return
     */
    public static byte[] generateEncryptedTokenWithPid(SecretKey kgSecretKey, String kylinPid) throws Exception {
        return encryptToken(kgSecretKey, kylinPid + "_" + System.currentTimeMillis());
    }

}