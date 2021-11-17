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
package io.kyligence.kap.secondstorage;

import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;

import io.kyligence.kap.metadata.cube.model.NDataflow;

public class NameUtil {

    private static final int UUID_LENGTH = RandomUtil.randomUUIDStr().length();
    public static final String TEMP_TABLE_FLAG = "temp";
    public static final String TEMP_SRC_TABLE_FLAG = "src";

    private NameUtil() {
    }

    public static String getDatabase(NDataflow df) {
        return getDatabase(df.getConfig(), df.getProject());
    }

    private static String databasePrefix(KylinConfig config) {
        return config.isUTEnv() ? "UT" : config.getMetadataUrlPrefix();
    }

    public static String getDatabase(KylinConfig config, String project) {
        return String.format(Locale.ROOT, "%s_%s", databasePrefix(config), project);
    }

    public static String getTable(NDataflow df, long layoutId) {
        return getTable(df.getUuid(), layoutId);
    }

    public static String getTable(String modelId, long layoutId){
        return String.format(Locale.ROOT, "%s_%d", tablePrefix(modelId), layoutId);
    }

    public static String tablePrefix(String modelId) {
        return modelId.replace("-", "_");
    }

    // reverse
    public static String recoverProject(String database, KylinConfig config) {
        return database.substring(databasePrefix(config).length() + 1);
    }

    public static Pair<String, Long> recoverLayout(String table) {
        String model = table.substring(0, UUID_LENGTH).replace("_", "-");
        Long layout = Long.parseLong(table.substring(UUID_LENGTH + 1));
        return Pair.newPair(model, layout);
    }

    public static boolean isTempTable(String table) {
        return table.contains(TEMP_TABLE_FLAG) || table.contains(TEMP_SRC_TABLE_FLAG);
    }
}
