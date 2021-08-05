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

import java.nio.charset.Charset;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;

import io.kyligence.kap.common.obf.IKeep;

public class EncryptUtil implements IKeep {
    /**
     * thisIsAsecretKey
     */
    private static final byte[] key = { 0x74, 0x68, 0x69, 0x73, 0x49, 0x73, 0x41, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74,
            0x4b, 0x65, 0x79 };

    public static final String ENC_PREFIX = "ENC('";
    public static final String ENC_SUBFIX = "')";

    public static final String DEC_FLAG = "DEC";

    public static boolean isEncrypted(String value) {
        return StringUtils.isNotEmpty(value) && value.startsWith(ENC_PREFIX) && value.endsWith(ENC_SUBFIX);
    }

    public static String encrypt(String strToEncrypt) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            final SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            return Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes(Charset.defaultCharset())));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static String encryptWithPrefix(String value) {
        return ENC_PREFIX + encrypt(value) + ENC_SUBFIX;
    }

    public static String decrypt(String strToDecrypt) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
            final SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            return new String(cipher.doFinal(Base64.decodeBase64(strToDecrypt)), Charset.defaultCharset());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static String decryptPassInKylin(String value) {
        return decrypt(value.substring(ENC_PREFIX.length(), value.length() - ENC_SUBFIX.length()));
    }

    private static void printUsage() {
        System.out.println("Usage: java org.apache.kylin.common.util <your_password>");
    }

    public static String getDecryptedValue(String value) {
        if (isEncrypted(value)) {
            return decryptPassInKylin(value);
        }
        return value;
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            printUsage();
            Unsafe.systemExit(1);
        }

        String passwordTxt = args[0];
        // for encrypt password like LDAP password
        System.out.println(EncryptUtil.encrypt(passwordTxt));
    }
}
