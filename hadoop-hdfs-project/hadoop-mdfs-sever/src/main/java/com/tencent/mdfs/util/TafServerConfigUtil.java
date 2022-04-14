package com.tencent.mdfs.util;

import com.qq.cloud.taf.common.util.Config;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * create:lichunxiao
 * Date:2019-04-02
 */
public class TafServerConfigUtil {

    private static final Logger logger  = LoggerFactory.getLogger( TafServerConfigUtil.class );

    private static  Config conf;

    private static String set;

    static {
        try {
            conf = Config.parseFile(System.getProperty("config"));
            set  = conf.get("/taf/application<setdivision>", "");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


   public Config getServerConfig(){

       return null;
   }

   public static String getSetName(){
       return set;
   }
}
