package com.tencent.mdfs.util;



import com.qq.cloud.component.dcache.client.dcache.ProxyPrx;
import com.qq.cloud.component.dcache.client.kv.DataCoder;
import com.qq.cloud.component.dcache.client.kv.HessianDataCoder;
import com.qq.cloud.component.dcache.client.kv.RawCachedData;
import com.qq.cloud.taf.client.CommunicatorFactory;
import com.qq.cloud.taf.common.support.Holder;
import com.qq.thessian.io.Hessian2Input;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VnnDCacheUtil {
    static Logger logger = LoggerFactory.getLogger( VnnDCacheUtil.class );
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    static DataCoder dataCoder = new HessianDataCoder();
    public static final String DCACHE_OBJECT_NAME = "DCACHE_OBJECT_NAME";
    public static final String DCACHE_MODULE_NAME = "DCACHE_MODULE_NAME";
    public static final String DCACHE_MODULE_NAME_DEFAULT = "DataMapVnnClientNN";
    public static final String DCACHE_OBJECT_NAME_DEFAULT = "DCache.DataMapProxyServer.ProxyObj";
    public static final String DCACHE_EXPIRE_TIME_CLIENTNN = "DCACHE_EXPIRE_TIME_CLIENTNN";
    public static final int DCACHE_EXPIRE_TIME_CLIENTNN_DEFAULT = 24 * 60 * 60 ;//24 hours


    static ProxyPrx pro = null;
    static {
        pro = CommunicatorFactory.getInstance().getCommunicator(SeerUtil.getLocator()).stringToProxy(ProxyPrx.class,
                VnnConfigUtil.get(DCACHE_OBJECT_NAME, DCACHE_OBJECT_NAME_DEFAULT));
    }

    public static String selectByKey(String key) {
        try {
            VnnLogUtil.debug("select key:"+key );
            Holder<byte[]> holder = new Holder();
            Holder<Byte> version = new Holder();

            int rst = pro.getStringWithVerBS(VnnConfigUtil.get( DCACHE_MODULE_NAME,DCACHE_MODULE_NAME_DEFAULT),key.getBytes(UTF_8),holder,version);
            VnnLogUtil.debug("select key: "+key + " result"+rst );

            if( rst == 0) {
                ByteArrayInputStream bin = new ByteArrayInputStream(holder.getValue());
                DataInputStream din = new DataInputStream(bin);
                Hessian2Input hessianInput = new Hessian2Input(din);

                try {
                    byte ver = din.readByte();
                    short flag = din.readShort();
                    Object resultObj = hessianInput.readObject();
                    VnnLogUtil.debug("select key: "+key + " result"+resultObj );

                    return (String)resultObj;
                } catch (IOException var8) {
                    throw new RuntimeException(var8);
                }

            }
            return null;
        } catch (Exception e) {
            logger.error("get from decache failed:{},",key,e);
            VnnLogUtil.err("get from decache failed:"+key);
            e.printStackTrace();
        }
        return null;
    }

    public static void saveKey(String key,String value){
        VnnLogUtil.debug("key:"+key + "---->value:"+value);
        if(key == null || value == null || key.trim().length() == 0){
            logger.error("save failed:{},{}",key,value);
            VnnLogUtil.err("save failed:"+key+ "," +value);
            return ;
        }
        try {
            RawCachedData rawData = dataCoder.encode(value);
            byte[] data = rawData.getData();
            int expireTimeSecond = ((int)System.currentTimeMillis()/1000) + DCACHE_EXPIRE_TIME_CLIENTNN_DEFAULT;

            int ret = pro.setStringExBS(VnnConfigUtil.get(DCACHE_MODULE_NAME, DCACHE_MODULE_NAME_DEFAULT),
                    key.getBytes(UTF_8), data,(byte)0,true, expireTimeSecond);
            VnnLogUtil.debug("save ret:"+ret+", key:"+key + "---->value:"+value + "save success." + " expire time:"+
                VnnConfigUtil.getIntValue(DCACHE_EXPIRE_TIME_CLIENTNN,DCACHE_EXPIRE_TIME_CLIENTNN_DEFAULT));
        }catch (Exception e){
            logger.error("save failed:{}",e);
            VnnLogUtil.err("save failed.");
            e.printStackTrace();
        }
    }

    public static void removeKey(String key){
        pro.delString(VnnConfigUtil.get( DCACHE_MODULE_NAME,DCACHE_MODULE_NAME_DEFAULT),key);
    }
}
