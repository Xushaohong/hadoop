package com.tencent.mdfs;

import com.tencent.mdfs.nnproxy.impl.CloudHdfsServer;
import com.tencent.mdfs.util.VnnConfigUtil;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mdfs {
  private static final Logger logger = LoggerFactory.getLogger(Mdfs.class);

  public static void main(String[] args) {

    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          logger.info(" try to start CloudHdfsServer");
          new CloudHdfsServer(VnnConfigUtil.getHadoopConfig()).start();
        } catch (Throwable e) {
          logger.error("CloudHdfsServer start error "+e);
          e.printStackTrace();
        }
        System.out.println("appContextStart successed");
        logger.info("appContextStart success...");
      }
    }).start();
  }
}
