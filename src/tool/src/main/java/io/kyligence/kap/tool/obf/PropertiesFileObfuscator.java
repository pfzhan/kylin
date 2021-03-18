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
package io.kyligence.kap.tool.obf;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Properties;

import lombok.val;

public abstract class PropertiesFileObfuscator extends FileObfuscator {
    protected PropertiesFileObfuscator(ObfLevel level, MappingRecorder recorder, ResultRecorder resultRecorder) {
        super(level, recorder, resultRecorder);
    }

    abstract void obfuscateProperties(Properties input);

    @Override
    void doObfuscateFile(File orig) {
        String path = orig.getAbsolutePath();
        Properties p = readProperties(orig);
        try (BufferedWriter bw = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(orig), Charset.defaultCharset()))) {
            for (val kv : p.entrySet()) {
                bw.write(kv.getKey() + "=" + kv.getValue());
                bw.newLine();
            }
            bw.flush();
            logger.info("{} processed successfully", path);
            resultRecorder.addSuccessFile(path);
        } catch (IOException e) {
            logger.info("{} processed failed", path);
            resultRecorder.addFailedFile(path);
        }
    }

    private Properties readProperties(File orig) {
        Properties p = new Properties();
        try (InputStream in = new BufferedInputStream(new FileInputStream(orig))) {
            p.load(in);
            obfuscateProperties(p);
        } catch (Exception e) {
            logger.info("Failed to load properties file:{}.", orig.getAbsolutePath(), e);
        }
        return p;
    }
}
