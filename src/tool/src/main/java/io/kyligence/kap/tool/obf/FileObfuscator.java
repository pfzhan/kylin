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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FileObfuscator extends AbstractObfuscator {
    protected static final Logger logger = LoggerFactory.getLogger("obfuscation");

    FileObfuscator(ObfLevel level, MappingRecorder recorder, ResultRecorder resultRecorder) {
        super(level, recorder, resultRecorder);
    }

    public void obfuscate(File orig, FileFilter fileFilter) {
        if (orig == null || !orig.exists())
            return;

        if (orig.isDirectory()) {
            obfuscateDirectory(orig, fileFilter);
        } else {
            obfuscateFile(orig);
        }
    }

    private void obfuscateDirectory(File directory, FileFilter fileFilter) {
        File[] files = fileFilter == null ? directory.listFiles() : directory.listFiles(fileFilter);
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    obfuscateDirectory(f, fileFilter);
                } else {
                    obfuscateFile(f);
                }
            }
        }
    }

    protected void obfuscateFile(File orig) {
        if (orig == null) {
            return;
        }

        String path = orig.getAbsolutePath();

        logger.info("Start to process file: {}", path);

        try {
            switch (level) {
            case OBF:
                doObfuscateFile(orig);
                break;
            case RAW:
                break;
            default:
                break;
            }
            logger.info("{} process successfully", path);
            resultRecorder.addSuccessFile(path);
        } catch (Exception e) {
            logger.warn("{} processed failed", path);
            resultRecorder.addFailedFile(path);
        }
    }

    abstract void doObfuscateFile(File orig) throws IOException;

}
