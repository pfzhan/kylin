package io.kyligence.kap.rest;

import java.io.IOException;

import org.apache.kylin.storage.hbase.util.DeployCoprocessorCLI;

public class DebugDeployCLI {

    public static void main(String args[]) throws IOException {
        DebugTomcat.setupDebugEnv();
        DeployCoprocessorCLI.main(new String[] {"default", "all"});
    }
}
