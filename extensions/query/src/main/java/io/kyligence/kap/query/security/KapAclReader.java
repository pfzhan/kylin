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

package io.kyligence.kap.query.security;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KapAclReader {
    private static final Logger logger = LoggerFactory.getLogger(KapAclReader.class);

    public static final String CSV_SPLIT = " ";
    public static final String DENY = "no";
    public static final String YES = "yes";

    private String filename;
    private long preLastModified = 0L;
    public Hashtable<String, Hashtable<String, String>> accessControlColumnsByUser = new Hashtable<String, Hashtable<String, String>>();
    public ArrayList<String> allAclColumns = new ArrayList<>();

    public KapAclReader() {
        String path;
        if (null != System.getenv("KYLIN_HOME"))
            path = System.getenv("KYLIN_HOME") + File.separator + "conf";
        else
            path = System.getProperty(KylinConfig.KYLIN_CONF);

        filename = path + File.separator + KapConfig.getInstanceFromEnv().getCellLevelSecurityConfig();
    }

    public void loadAndValidateACL(DataModelDesc dmDesc) {
        if (isFileChanged()) {
            try {
                loadFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            verifyColumns(dmDesc);
        }
    }

    public boolean isAvailable(String curUser) {
        if (0 == accessControlColumnsByUser.size())
            return false;
        if (null == accessControlColumnsByUser.get(curUser.toLowerCase()))
            return false;
        return true;
    }

    private void verifyColumns(DataModelDesc model) {
        if (null == model)
            return;

        for (String colName : allAclColumns) {
            try {
                model.findColumn(colName);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("The columns name: \"" + colName
                        + "\" in cell-level-security config is incorrect, please revise it", ex);
            }
        }
    }

    public void loadFile() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(this.filename));
        String line;
        try {
            while (null != (line = reader.readLine())) {
                if (!line.trim().startsWith("#") && !line.equals(""))
                    break;
            }
            if (null == line) {
                return;
            }

            clearAll();
            String[] cols = line.split(CSV_SPLIT);
            if (cols.length <= 1) {
                logger.error("The schema of cell-level-security config file is not correct, it doesn't work!");
                return;
            }
            //since the first column is KAP login user, will be excluded
            for (int i = 1; i < cols.length; i++)
                allAclColumns.add(cols[i]);
            //scan acl table
            while (null != (line = reader.readLine())) {
                if (line.trim().startsWith("#") && !line.equals(""))
                    continue;
                Hashtable<String, String> limitedColumns = new Hashtable<>();
                String[] values = line.split(CSV_SPLIT);
                for (int i = 0; i < values.length; i++) {
                    limitedColumns.put(cols[i].toLowerCase(), values[i].trim());
                }
                accessControlColumnsByUser.put(values[0].toLowerCase(), limitedColumns);
            }
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    public boolean isFileChanged() {
        long last = lastModified();
        if (preLastModified != last) {
            preLastModified = last;
            return true;
        }
        return false;
    }

    public long lastModified() {
        File file = new File(this.filename);
        if (!file.exists()) {
            logger.warn("Can not find acl file: " + filename + ", which is specified in kylin.properties");
            return 0L;
        }
        long last = file.lastModified();
        return last;
    }

    public void clearAll() {
        for (Map.Entry<String, Hashtable<String, String>> columnPair : accessControlColumnsByUser.entrySet()) {
            columnPair.getValue().clear();
        }
        accessControlColumnsByUser.clear();
        allAclColumns.clear();
    }
}
