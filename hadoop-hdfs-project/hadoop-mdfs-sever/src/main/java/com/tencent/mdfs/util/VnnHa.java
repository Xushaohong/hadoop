package com.tencent.mdfs.util;

import com.tencent.mdfs.entity.RouteInfo;
import com.tencent.mdfs.nnproxy.context.ContextUtil;
import com.tencent.mdfs.provider.util.DaoUtil;
import com.tencent.mdfs.router.RouteServiceManager;
import java.net.ConnectException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VnnHa {
    private static final ThreadPoolExecutor pool = new ThreadPoolExecutor(1,
            1,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(10000),
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "update-nnip" + threadNumber.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            }, new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            logger.error("too much update request  ");
        }
    });
    static Logger logger = LoggerFactory.getLogger( VnnHa.class );

    private static void refreshAddr(String clusterId, String src){
        VnnLogUtil.log("refreshAddr " + clusterId + " src:" +src);
        try {
            if( src != null && VnnPathUtil.isMDFS(src) ){
                RouteInfo rst = RouteServiceManager.getInstance().route( src );
                if( rst != null ) {
                    clusterId = rst.getClusterId();
                }
            }

            if( clusterId == null ){
                if( src == null ){
                    String addr = ContextUtil.getDefaultAddr();
                    clusterId = LocalCache.getClusterId( addr );
                }else if( src != null && !VnnPathUtil.isMDFS(src) ){
                    String nnAddr = VnnPathUtil.getNNAddr(src);
                    if( nnAddr != null ){
                        clusterId = LocalCache.getClusterId( nnAddr );
                    }
                }
            }
            if( clusterId == null ){
                logger.error( "can not find clusterId:{}",src );
                return;
            }

            LocalCache.NnInfo nns = LocalCache.nnChoises.get(clusterId);
            if (nns == null || nns.getNnAddrs() == null || nns.getNnAddrs().size() == 0) {
                return ;
            }
            if( nns.getCurrentIndex() > -1 ){
                nns.getLastFailTime().put( nns.getCurrentIndex(),System.currentTimeMillis() );
            }
            int index = (nns.getCurrentIndex() + 1) % (nns.getNnAddrs().size());
            int originIndex = index;
            while (nns.getLastFailTime().get(index) != null && System.currentTimeMillis() - nns.getLastFailTime().get(index) < 5000) {
                //return nns.get(0);
                index = (index + 1) % (nns.getNnAddrs().size());
                if (index == originIndex) {
                    return ;
                }
            }
            final String addr = nns.getNnAddrs().get(index);
            nns.setCurrentIndex(index);
            LocalCache.nnAddrCache.put( clusterId ,addr );

            final  String cluster = clusterId;
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    String ip = addr;
                    if( addr != null && addr.indexOf(":") > -1 ){
                        ip = addr.split(":")[0];
                    }
                    Boolean rst = DaoUtil.updateNNIp( cluster,ip );
                    logger.info("update nn rst:{},{},{}",cluster,ip,rst);
                }
            });

        }catch ( Exception e){
            logger.warn("get refreshAddr nnaddr failed",e);
        }
    }

    public static boolean refreshAddr(Throwable cause){
        boolean success = false;
        if( cause instanceof  RemoteException){
            RemoteException re = (RemoteException) cause;

            if(  cause != null && (StandbyException.class.getName().equals( re.getClassName() ) ||
                    RetriableException.class.getName().equals(re.getClassName() )) ) {
                String clusterId = null;
                refreshAddr(clusterId,ThreadLocalUtil.src.get());
                success = true;
            }
        }else if( cause instanceof ConnectException ){
            String clusterId = null;
            refreshAddr(clusterId,ThreadLocalUtil.src.get());
            success = true;
        }else{
            VnnLogUtil.log("not standbyException" + cause.getMessage());
        }
        return success;
    }
}
