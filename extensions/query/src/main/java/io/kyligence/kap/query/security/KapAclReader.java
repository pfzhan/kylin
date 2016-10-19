package io.kyligence.kap.query.security;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
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

    private void verifyColumns(DataModelDesc dmDesc) {
        if (null == dmDesc)
            return;

        for (String colName : allAclColumns) {
            boolean isColumnsInFact = false;
            boolean isColumnsInLookups = false;
            for (ColumnDesc colDesc : dmDesc.getFactTableDesc().getColumns()) {
                String nameInTable = colDesc.getTable().getName() + "." + colDesc.getName();
                if (nameInTable.equalsIgnoreCase(colName)) {
                    isColumnsInFact = true;
                    break;
                }
            }

            if (isColumnsInFact)
                continue;

            for (TableDesc tableDesc : dmDesc.getLookupTableDescs()) {
                for (ColumnDesc colDesc : tableDesc.getColumns()) {
                    String nameInTable = colDesc.getTable().getName() + "." + colDesc.getName();
                    if (nameInTable.equalsIgnoreCase(colName)) {
                        isColumnsInLookups = true;
                        break;
                    }
                }
            }
            if (!isColumnsInFact && !isColumnsInLookups)
                throw new IllegalArgumentException("The columns name: \"" + colName + "\" in cell-level-security config is incorrect, please revise it");
        }
    }

    public void loadFile() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(this.filename));
        String line;
        while (null != (line = reader.readLine())) {
            if (!line.trim().startsWith("#") && !line.equals(""))
                break;
        }
        if (null == line) {
            reader.close();
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
        reader.close();
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
