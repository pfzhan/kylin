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

package io.kyligence.kap.ext.classloader;

import java.io.File;
import java.net.URLClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClassLoaderUtils {
    static URLClassLoader sparkClassLoader = null;
    static URLClassLoader originClassLoader = null;
    private static Logger logger = LoggerFactory.getLogger(ClassLoaderUtils.class);

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

    public static ClassLoader getSparkClassLoader() {
        if (sparkClassLoader == null) {
            logger.error("sparkClassLoader not init");
            return Thread.currentThread().getContextClassLoader();
        } else {
            return sparkClassLoader;
        }
    }

    public static void setSparkClassLoader(URLClassLoader classLoader) {
        sparkClassLoader = classLoader;
    }

    public static ClassLoader getOriginClassLoader() {
        if (originClassLoader == null) {
            logger.error("sparkClassLoader not init");
            return Thread.currentThread().getContextClassLoader();
        } else {
            return originClassLoader;
        }
    }

    public static void setOriginClassLoader(URLClassLoader classLoader) {
        originClassLoader = classLoader;
    }
}
