package com.tencent.mdfs.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class MigLogUtil {

    public static final String PATTERN_YYYYMMDDHHMMSS = "yyyy-MM-dd HH:mm:ss";
    public static final SimpleDateFormat format = new SimpleDateFormat(PATTERN_YYYYMMDDHHMMSS);

    private static Log LOG= LogFactory.getLog("MigLogUtil");


    private final static boolean DEBUG=false;

    public static void log(Object obj){

        //System.out.println(format(obj));
        LOG.info(obj);
    }

    public static void err(Object obj){
        //System.out.println(format(obj));
        //System.err.println(format(obj));
        LOG.error(obj);
    }

    private static String format(Object obj){
       return format(new Date())+" "+Thread.currentThread().getName()+" "+obj;
    }

    public static String format(Date date) {
        return format.format(date);
    }

    public static void debug(Object obj) {
        //System.err.println(format(obj));
        if(LOG.isDebugEnabled()){
            LOG.debug(obj);
        }

    }

    public static void warn(Object obj) {
        LOG.warn(obj);
    }
}
