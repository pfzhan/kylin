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

package io.kyligence.kap.engine.spark.application;

import io.kyligence.kap.common.obf.IKeep;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.utils.LOGUtils;

/**
 */
public final class SparkEntry implements IKeep {


    public static void main(String[] args) {
        try {
            LOGUtils.initLog();
            LOGUtils.info("SparkEntry args:" + StringUtils.join(args, " "));
            if (args.length < 2) {
                throw new IllegalArgumentException("-className is required");
            }
            if (!(args[0].equals("-className"))) {
                throw new IllegalArgumentException("-className is required");
            }
            final String className = args[1];
            final Object o = Class.forName(className).newInstance();
            if (!(o instanceof SparkApplication)) {
                LOGUtils.info(className + " is not a subClass of AbstractApplication");
                System.exit(1);
            }
            String[] appArgs = new String[args.length - 2];
            System.arraycopy(args, 2, appArgs, 0, args.length - 2);
            SparkApplication abstractApplication = (SparkApplication) o;
            abstractApplication.execute(appArgs);
            System.exit(0);
        } catch (Throwable th) {
            LOGUtils.error("SparkEntry exec error:", th);
            System.exit(1);
        }
    }
}
