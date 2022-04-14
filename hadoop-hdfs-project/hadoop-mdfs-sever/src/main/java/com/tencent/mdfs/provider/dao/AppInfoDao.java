package com.tencent.mdfs.provider.dao;

import com.tencent.mdfs.provider.orm.AppInfoMapper;
import com.tencent.mdfs.provider.pojo.AppInfo;
import com.tencent.mdfs.provider.util.MyBatisUtil;
import java.util.List;
import javax.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * dao */
@Resource
public class AppInfoDao {
  private static final Logger logger = LoggerFactory.getLogger(AppInfoDao.class);

  private final AppInfoMapper appInfoMapper= MyBatisUtil.getMapper(AppInfoMapper.class);

  public Integer insert(AppInfo arg0) {
    try{
      return getMapper().insert(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public Integer insertSelective(AppInfo arg0) {

    try{
      return getMapper().insertSelective(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }


  public AppInfo selectByPrimaryKey(Integer arg0) {
    try{
      return getMapper().selectByPrimaryKey(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public List<AppInfo> selectByPrimaryKeyList(List<Integer> arg0) {

    try{
      return getMapper().selectByPrimaryKeyList(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public Integer updateByPrimaryKey(AppInfo arg0) {

    try{
      return getMapper().updateByPrimaryKey(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public Integer updateByPrimaryKeySelective(AppInfo arg0) {

    try{
      return getMapper().updateByPrimaryKeySelective(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public List<AppInfo> selectAll() {

    try{
      return getMapper().selectAll();
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }
  
  private AppInfoMapper getMapper(){
    return MyBatisUtil.getMapper(AppInfoMapper.class);
  } 
}
