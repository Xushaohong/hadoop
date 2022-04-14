package com.tencent.mdfs.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * create:lichunxiao
 * Date:2019-03-14
 */
public class TafLogUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger("taf_flow");

    public static void debug(String s){
       if(LOGGER.isDebugEnabled()){
           LOGGER.debug(s);
       }
    }

    public static void log(String s){
        LOGGER.info(s);
    }

    public static void info(String s){
        LOGGER.info(s);
    }



    public static void err(Object obj){
        LOGGER.error(obj+"");
    }

    public static void warn(Object obj) {
        LOGGER.warn(obj+"");
    }


}
