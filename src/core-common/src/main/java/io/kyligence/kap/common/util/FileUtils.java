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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.kylin.common.util.OrderedProperties;

import com.google.common.base.Preconditions;

import lombok.val;

public final class FileUtils {
    public static File findFile(String dir, String ptn) {
        File[] files = new File(dir).listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.getName().matches(ptn))
                    return f;
            }
        }
        return null;
    }

    public static Map<String, String> readFromPropertiesFile(InputStream inputStream) {
        try (BufferedReader confReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))) {
            OrderedProperties temp = new OrderedProperties();
            temp.load(confReader);
            return temp.getProperties();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static String encodeBase64File(String path) throws Exception {
        File file = new File(path);
        try (FileInputStream inputFile = new FileInputStream(file)) {
            byte[] buffer = new byte[(int) file.length()];
            final int read = inputFile.read(buffer);
            Preconditions.checkState(read != -1);
            return Base64.encodeBase64String(buffer);
        }
    }

    public static void decoderBase64File(String base64Code, String targetPath) throws Exception {
        val buffer = Base64.decodeBase64(base64Code);
        try (FileOutputStream out = new FileOutputStream(targetPath)) {
            out.write(buffer);
        }
    }
}
