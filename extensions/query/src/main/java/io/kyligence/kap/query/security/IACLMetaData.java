package io.kyligence.kap.query.security;

import java.io.IOException;
import java.util.ArrayList;

public interface IACLMetaData {
    public static final String CSV_SPLIT = " ";
    public static final String DENY = "no";
    public static final ACLAttribution limitedColumnsByUser = new ACLAttribution();
    public static final ArrayList<String> allAclColumns = new ArrayList<>();

    public static enum metadataSource {
        ACL, DataBase;
    }

    /*
    * Load acl file
    * @return {false,true}
    */
    public boolean loadACLFile() throws IOException;
}
