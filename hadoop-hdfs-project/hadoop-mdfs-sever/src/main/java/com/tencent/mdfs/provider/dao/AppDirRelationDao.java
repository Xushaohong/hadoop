package com.tencent.mdfs.provider.dao;

import com.tencent.mdfs.provider.orm.AppDirRelationMapper;
import com.tencent.mdfs.provider.pojo.AppDirRelation;
import com.tencent.mdfs.provider.util.MyBatisUtil;
import java.util.List;
import javax.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * dao */
@Resource
public class AppDirRelationDao {
  private static final Logger logger = LoggerFactory.getLogger(AppDirRelationDao.class);

  private AppDirRelationMapper appDirRelationMapper= MyBatisUtil.getMapper(AppDirRelationMapper.class);

  public Integer insert(AppDirRelation arg0) {

    try{
      return getMapper().insert(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public Integer insertSelective(AppDirRelation arg0) {

    try{
      return getMapper().insertSelective(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public AppDirRelation selectByPrimaryKey(Integer arg0) {

    try{
      return getMapper().selectByPrimaryKey(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public Integer updateByPrimaryKey(AppDirRelation arg0) {

    try{
      return getMapper().updateByPrimaryKey(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public Integer updateByPrimaryKeySelective(AppDirRelation arg0) {

    try{
      return getMapper().updateByPrimaryKeySelective(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public List<AppDirRelation> selectAll() {
    try{
      return getMapper().selectAll();
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  private AppDirRelationMapper getMapper(){
    return MyBatisUtil.getMapper(AppDirRelationMapper.class);
  }
}
