package com.tencent.mdfs.util;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.map.LRUMap;

public class ClientNnCacheUtil {
  private final static Map<String,Set<String>> clientNNMap = Collections.synchronizedMap(new LRUMap(100000));

  public static void save(String clientName,String value){
    try {
      Set<String> ips = get(clientName);
      ips.add(value);
      VnnDCacheUtil.saveKey(clientName, set2String(ips));
      clientNNMap.put(clientName,ips);
    }catch (Throwable e){
      VnnLogUtil.err("save failed. "+clientName + "-->" +value);
      e.printStackTrace();
    }
  }
  public static void remove(String clientName,String value){
    //do nothing 当一个客户端同事打开多个文件时不能remove，这里使用LRU淘汰的方式
    /*try {
      Set<String> ips = get(clientName);
      ips.remove(value);
      if(ips.size() ==0) {
        VnnDCacheUtil.removeKey(clientName);
        clientNNMap.remove(clientName);
      }else{
        VnnDCacheUtil.saveKey(clientName,set2String(ips));
        clientNNMap.put(clientName,ips);
      }
    }catch (Throwable e){
      VnnLogUtil.err("remove failed. "+clientName );

      e.printStackTrace();
    }*/
  }
  public static Set<String> get(String key){
    try {
      String value = VnnDCacheUtil.selectByKey(key);
      if (value == null) {
        if (clientNNMap.containsKey(key)) {
          return clientNNMap.get(key);
        } else {
          VnnLogUtil.warn("select null " + key);
        }
      }
      return string2Set(value);
    }catch (Throwable e){
      VnnLogUtil.err("get failed. "+key);
      e.printStackTrace();
      if (clientNNMap.containsKey(key)) {
        return clientNNMap.get(key);
      }
      return null;
    }
  }
  public static int getLocalSize(String clientName){
    Set<String> nns = clientNNMap.get(clientName);
    return  nns == null ? 0 : nns.size();
  }

  public static int getLocalSize() {
    return clientNNMap.size();
  }

  private static Set<String> string2Set(String nns){
    Set<String> rst = new LinkedHashSet<>();
    if( nns == null ){
      return rst;
    }else{
      String nnArr[] = nns.split(",");
      for(String nnStr : nnArr) {
        rst.add(nnStr);
      }
    }
    return rst;
  }

  private static String set2String(Set<String> nnSet){
    if(nnSet == null || nnSet.size() == 0){
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for(String nn : nnSet){
      sb.append(nn).append(",");
    }
    return sb.substring(0,sb.length() -1);
  }

  public static void main(String[] args) {
    String nns = "10.0.0.1:9000";
    Set<String> nnSet = string2Set(nns);
    nnSet.add("10.0.0.3:9000");
    String nnStr = set2String(nnSet);
    System.out.println(nnStr);
  }


}
