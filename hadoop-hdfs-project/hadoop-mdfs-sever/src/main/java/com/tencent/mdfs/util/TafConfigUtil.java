package com.tencent.mdfs.util;


import com.google.common.base.Strings;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * create:chunxiaoli
 * Date:5/8/18
 */
public class TafConfigUtil {
  private static String localIp;

    public static String getCalleeIp(){
       return getLocalIp();
    }


  public static String getLocalIp() {
    if (Strings.isNullOrEmpty(localIp)) {
      InetAddress ipAddr = null;

      try {
        ipAddr = InetAddress.getLocalHost();
        localIp = ipAddr.getHostAddress();
        System.out.println("local ip is ..." + localIp);
        return localIp;
      } catch (UnknownHostException var2) {
        var2.printStackTrace();
        return "";
      }
    } else {
      return localIp;
    }
  }

}
