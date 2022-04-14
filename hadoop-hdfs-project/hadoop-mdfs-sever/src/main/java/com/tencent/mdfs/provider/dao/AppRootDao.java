package com.tencent.mdfs.provider.dao;

import com.tencent.mdfs.provider.orm.AppRootMapper;
import com.tencent.mdfs.provider.pojo.AppRoot;
import com.tencent.mdfs.provider.util.MyBatisUtil;
import java.util.List;
import javax.annotation.Resource;

@Resource
public class AppRootDao {


    public List<AppRoot> listAll(){
        try{
            return getMapper().listAll();
        }finally {
            MyBatisUtil.closeSqlSession();
        }
    }

    private AppRootMapper getMapper(){
        return MyBatisUtil.getMapper(AppRootMapper.class);
    }
}
