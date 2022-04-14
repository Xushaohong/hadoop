package com.tencent.mdfs.provider.dao.impl;

import com.tencent.mdfs.provider.orm.AppDirRelationMapper;
import com.tencent.mdfs.provider.pojo.AppDirRelation;
import javax.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * dao impl */
public class AppDirRelationDaoImpl {
  private static final Logger logger = LoggerFactory.getLogger(AppDirRelationDaoImpl.class);

  @Resource
  private AppDirRelationMapper appDirRelationMapper;

  public Integer insert(AppDirRelation arg0) {
    return appDirRelationMapper.insert(arg0);
  }

  public Integer insertSelective(AppDirRelation arg0) {
    return appDirRelationMapper.insertSelective(arg0);
  }

  public AppDirRelation selectByPrimaryKey(Integer arg0) {
    return appDirRelationMapper.selectByPrimaryKey(arg0);
  }

  public Integer updateByPrimaryKey(AppDirRelation arg0) {
    return appDirRelationMapper.updateByPrimaryKey(arg0);
  }

  public Integer updateByPrimaryKeySelective(AppDirRelation arg0) {
    return appDirRelationMapper.updateByPrimaryKeySelective(arg0);
  }
}
