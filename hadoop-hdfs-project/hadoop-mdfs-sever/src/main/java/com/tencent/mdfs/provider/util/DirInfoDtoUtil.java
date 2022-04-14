package com.tencent.mdfs.provider.util;

import com.tencent.mdfs.provider.dto.DirInfoDto;
import com.tencent.mdfs.provider.pojo.DirInfo;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirInfoDtoUtil {
  private static final Logger logger = LoggerFactory.getLogger(DirInfoDtoUtil.class);

  public static DirInfoDto convertToDto(DirInfo obj) {
    if(obj==null){
      return null;
    }
    //logger.debug("pojo:{}",obj);
    DirInfoDto ret=new DirInfoDto();
    ret.setDirId(obj.getDirId());
    ret.setAppId(obj.getAppId());
    ret.setLogicPath(obj.getLogicPath());
    ret.setRealPath(obj.getRealPath());
    ret.setType(obj.getType());
    ret.setShareType(obj.getShareType());
    ret.setShareType(obj.getShareType());
    ret.setNamenodeAddr(obj.getNamenodeAddr());
    ret.setCreateTime(obj.getCreateTime());
    ret.setUpdateTime(obj.getUpdateTime());
    ret.setState(obj.getState());
    ret.setClusterInfos(obj.getClusterInfos());
    ret.setClusterId(obj.getClusterId());
    return ret;
  }

  public static DirInfo convertToPojo(DirInfoDto obj) {
    //logger.debug("pojo:{}",obj);
    if(obj==null){
      return null;
    }
    DirInfo ret=new DirInfo();
    ret.setDirId(obj.getDirId());
    ret.setAppId(obj.getAppId());
    ret.setLogicPath(obj.getLogicPath());
    ret.setRealPath(obj.getRealPath());
    ret.setType(obj.getType());
    ret.setNamenodeAddr(obj.getNamenodeAddr());
    ret.setCreateTime(obj.getCreateTime());
    ret.setUpdateTime(obj.getUpdateTime());
    ret.setState(obj.getState());
    ret.setShareType(obj.getShareType());
    ret.setClusterInfos(obj.getClusterInfos());
    ret.setClusterId(obj.getClusterId());
    return ret;
  }

  public static List<DirInfoDto> convertToDto(List<DirInfo> infos) {
    List<DirInfoDto> ret = new ArrayList<>();
    if(infos==null){
      return ret;
    }
    for(DirInfo info:infos){
      ret.add(convertToDto(info));
    }
    return ret;
  }

  public static List<DirInfo> convertToPojo(List<DirInfoDto> infos) {
    List<DirInfo> ret = new ArrayList<>();
    if(infos==null){
      return ret;
    }
    for(DirInfoDto info:infos){
      ret.add(convertToPojo(info));
    }
    return ret;
  }
}
