package com.tencent.mdfs.provider.dao;


import com.tencent.mdfs.provider.orm.AppRelationMapper;
import com.tencent.mdfs.provider.pojo.AppRelation;
import com.tencent.mdfs.provider.util.MyBatisUtil;
import java.util.List;
import javax.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Resource
public class AppRelationDao {
    private static final Logger logger = LoggerFactory.getLogger(AppRelationDao.class);

    private AppRelationMapper appDirRelationMapper= MyBatisUtil.getMapper(AppRelationMapper.class);

    public List<AppRelation> selectAll() {
        try{
            return getMapper().selectAll();
        }finally {
            MyBatisUtil.closeSqlSession();
        }
    }

    private AppRelationMapper getMapper(){
        return MyBatisUtil.getMapper(AppRelationMapper.class);
    }

}
