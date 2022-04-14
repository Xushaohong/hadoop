package com.tencent.mdfs.util;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;


/**
 * create:lichunxiao
 * Date:2019-03-14
 */
public class VnnLogUtil {

    private static final Logger LOGGER = LogManager.getLogger("VnnLogUtil");

    public static void debug(String s){
       if(LOGGER.isDebugEnabled()){
           LOGGER.info(s);
       }
    }

    public static void log(String s){
        LOGGER.info(s);
    }

    public static void err(Throwable e){
        LOGGER.error("",e);
    }

    public static void setLogLevel(String logger,String level){
        try{
            Configurator.setLevel(logger, Level.valueOf(level));
        }catch (Exception e){
            e.printStackTrace();
            LOGGER.error("setLogLevel error "+logger+" "+level);
        }
    }

    public static void err(Object obj){
        LOGGER.error(obj);
    }

    public static void warn(Object obj) {
        LOGGER.warn(obj);
    }

    public static void stdout(Object obj){

    }

    public static void err(String msg, Exception e) {
        LOGGER.error(msg,e);
    }
}
