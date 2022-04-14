package com.tencent.mdfs.provider.dao;


import com.tencent.mdfs.provider.orm.DirInfoMapper;
import com.tencent.mdfs.provider.pojo.DirInfo;
import com.tencent.mdfs.provider.util.MyBatisUtil;
import java.util.List;
import javax.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * dao */
@Resource
public class DirInfoDao {
  private static final Logger logger = LoggerFactory.getLogger(DirInfoDao.class);
  private DirInfoMapper dirInfoMapper= MyBatisUtil.getMapper(DirInfoMapper.class);

  public Integer insert(DirInfo arg0) {
    try{
      return getMapper().insert(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }

  }

  public Integer batchInsert(List<DirInfo> arg0) {
    try{
      return getMapper().batchInsert(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }

  }

  public Integer insertSelective(DirInfo arg0) {


    try{
      return getMapper().insertSelective(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public DirInfo selectByPrimaryKey(Integer arg0) {

    try{
      return getMapper().selectByPrimaryKey(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public Integer updateByPrimaryKey(DirInfo arg0) {


    try{
      return getMapper().updateByPrimaryKey(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public Integer updateByPrimaryKeySelective(DirInfo arg0) {


    try{
      return getMapper().updateByPrimaryKeySelective(arg0);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  public List<DirInfo> selectAll() {

    try{
      return getMapper().selectAll();
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }

  private DirInfoMapper getMapper(){
    return MyBatisUtil.getMapper(DirInfoMapper.class);
  }

  public DirInfo dirInfoSelectByConditions(DirInfo pojo) {
    try{
      return getMapper().dirInfoSelectByConditions(pojo);
    }finally {
      MyBatisUtil.closeSqlSession();
    }
  }
}
