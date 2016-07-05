package io.kyligence.kap.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class License {
    
    private static final Logger logger = LoggerFactory.getLogger("License");

    public static void info(String msg) {
        logger.info(msg);
    }
    
    public static void error(String msg) {
        logger.error(msg);
        System.exit(-10);
    }
    
    public static void error(String msg, Throwable ex) {
        logger.error(msg, ex);
        System.exit(-10);
    }
}
