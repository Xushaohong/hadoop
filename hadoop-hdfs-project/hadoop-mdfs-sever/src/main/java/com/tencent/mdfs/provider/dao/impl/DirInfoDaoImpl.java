package com.tencent.mdfs.provider.dao.impl;


import com.tencent.mdfs.provider.orm.DirInfoMapper;
import com.tencent.mdfs.provider.pojo.DirInfo;
import javax.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * dao impl */
public class DirInfoDaoImpl {
  private static final Logger logger = LoggerFactory.getLogger(DirInfoDaoImpl.class);

  @Resource
  private DirInfoMapper dirInfoMapper;

  public Integer insert(DirInfo arg0) {
    return dirInfoMapper.insert(arg0);
  }

  public Integer insertSelective(DirInfo arg0) {
    return dirInfoMapper.insertSelective(arg0);
  }

  public DirInfo selectByPrimaryKey(Integer arg0) {
    return dirInfoMapper.selectByPrimaryKey(arg0);
  }

  public Integer updateByPrimaryKey(DirInfo arg0) {
    return dirInfoMapper.updateByPrimaryKey(arg0);
  }

  public Integer updateByPrimaryKeySelective(DirInfo arg0) {
    return dirInfoMapper.updateByPrimaryKeySelective(arg0);
  }
}
