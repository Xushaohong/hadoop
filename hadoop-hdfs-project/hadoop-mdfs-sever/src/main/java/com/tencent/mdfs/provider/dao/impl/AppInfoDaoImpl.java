package com.tencent.mdfs.provider.dao.impl;

import com.tencent.mdfs.provider.orm.AppInfoMapper;
import com.tencent.mdfs.provider.pojo.AppInfo;
import javax.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * dao impl */
public class AppInfoDaoImpl {
  private static final Logger logger = LoggerFactory.getLogger(AppInfoDaoImpl.class);

  @Resource
  private AppInfoMapper appInfoMapper;

  public Integer insert(AppInfo arg0) {
    return appInfoMapper.insert(arg0);
  }

  public Integer insertSelective(AppInfo arg0) {
    return appInfoMapper.insertSelective(arg0);
  }

  public AppInfo selectByPrimaryKey(Integer arg0) {
    return appInfoMapper.selectByPrimaryKey(arg0);
  }

  public Integer updateByPrimaryKey(AppInfo arg0) {
    return appInfoMapper.updateByPrimaryKey(arg0);
  }

  public Integer updateByPrimaryKeySelective(AppInfo arg0) {
    return appInfoMapper.updateByPrimaryKeySelective(arg0);
  }
}
