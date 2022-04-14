package com.tencent.mdfs.provider.util;

import com.tencent.mdfs.provider.dto.AppDirRelationDto;
import com.tencent.mdfs.provider.pojo.AppDirRelation;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppDirRelationDtoUtil {
  private static final Logger logger = LoggerFactory.getLogger(AppDirRelationDtoUtil.class);

  public static AppDirRelationDto convertToDto(AppDirRelation obj) {
    if(obj==null){
      return null;
    }
    logger.debug("pojo:{}",obj);
    AppDirRelationDto ret=new AppDirRelationDto();
    ret.setId(obj.getId());
    ret.setAppId(obj.getAppId());
    ret.setDirId(obj.getDirId());
    ret.setType(obj.getType());
    ret.setCreateTime(obj.getCreateTime());
    ret.setUpdateTime(obj.getUpdateTime());
    ret.setState(obj.getState());
    return ret;
  }

  public static AppDirRelation convertToPojo(AppDirRelationDto obj) {
    logger.debug("pojo:{}",obj);
    if(obj==null){
      return null;
    }
    AppDirRelation ret=new AppDirRelation();
    ret.setId(obj.getId());
    ret.setAppId(obj.getAppId());
    ret.setDirId(obj.getDirId());
    ret.setType(obj.getType());
    ret.setCreateTime(obj.getCreateTime());
    ret.setUpdateTime(obj.getUpdateTime());
    ret.setState(obj.getState());
    return ret;
  }

  public static List<AppDirRelationDto> convertToDto(List<AppDirRelation> infos) {
    List<AppDirRelationDto> ret = new ArrayList<>();
    if(infos==null){
      return ret;
    }
    for(AppDirRelation info:infos){
      ret.add(convertToDto(info));
    }
    return ret;
  }
}
