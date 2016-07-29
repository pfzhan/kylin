package io.kyligence.kap.tool.general;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class CryptTool {
    private static byte[] key = { 0x74, 0x68, 0x69, 0x73, 0x49, 0x73, 0x41, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x4b, 0x65, 0x79 };

    public static String encrypt(String strToEncrypt) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            final SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            final String encryptedString = Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes()));
            return encryptedString;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java io.kyligence.kap.tool.general.CryptTool <EncryptMethod> <your_password>");
        System.out.println("EncryptMethod: AES");
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            printUsage();
            System.exit(1);
        }

        String encryptMethod = args[0];
        String passwordTxt = args[1];

        if ("AES".equalsIgnoreCase(encryptMethod)) {
            // for encrypt password like LDAP password
            System.out.println(encryptMethod + " encrypted password is: ");
            System.out.println(encrypt(passwordTxt));
        } else {
            printUsage();
            System.exit(1);
        }
    }
}
