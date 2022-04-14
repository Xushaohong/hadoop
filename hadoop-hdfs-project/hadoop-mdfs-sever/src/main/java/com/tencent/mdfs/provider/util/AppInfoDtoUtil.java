package com.tencent.mdfs.provider.util;

import com.tencent.mdfs.provider.dto.AppInfoDto;
import com.tencent.mdfs.provider.pojo.AppInfo;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppInfoDtoUtil {
  private static final Logger logger = LoggerFactory.getLogger(AppInfoDtoUtil.class);

  public static AppInfoDto convertToDto(AppInfo obj) {
    logger.debug("pojo:{}",obj);
    if(obj==null){
      return null;
    }
    AppInfoDto ret=new AppInfoDto();
    ret.setAppId(obj.getAppId());
    ret.setAppName(obj.getAppName());
    ret.setAppCode(obj.getAppCode());
    ret.setType(obj.getType());
    ret.setBg(obj.getBg());
    ret.setProduct(obj.getProduct());
    ret.setBusiness(obj.getBusiness());
    ret.setOwner(obj.getOwner());
    ret.setCreateTime(obj.getCreateTime());
    ret.setUpdateTime(obj.getUpdateTime());
    ret.setState(obj.getState());
    return ret;
  }

  public static List<AppInfoDto> convertToDto(List<AppInfo> infos) {

     List<AppInfoDto> ret = new ArrayList<>();
    if(infos==null){
      return ret;
    }
     for(AppInfo info:infos){
       ret.add(convertToDto(info));
     }
     return ret;
  }

  public static AppInfo convertToPojo(AppInfoDto obj) {
    if(obj==null){
      return null;
    }
    logger.debug("pojo:{}",obj);
    AppInfo ret=new AppInfo();
    ret.setAppId(obj.getAppId());
    ret.setAppName(obj.getAppName());
    ret.setAppCode(obj.getAppCode());
    ret.setType(obj.getType());
    ret.setBg(obj.getBg());
    ret.setProduct(obj.getProduct());
    ret.setBusiness(obj.getBusiness());
    ret.setOwner(obj.getOwner());
    ret.setCreateTime(obj.getCreateTime());
    ret.setUpdateTime(obj.getUpdateTime());
    ret.setState(obj.getState());
    return ret;
  }
}
