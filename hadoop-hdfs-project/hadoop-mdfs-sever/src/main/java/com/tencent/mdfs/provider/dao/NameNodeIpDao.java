package com.tencent.mdfs.provider.dao;

import com.tencent.mdfs.provider.orm.NameNodeIpMapper;
import com.tencent.mdfs.provider.pojo.NameNodeIp;
import com.tencent.mdfs.provider.util.BDMSMyBatisUtil;

public class NameNodeIpDao {
    public boolean update(NameNodeIp nameNodeIp){
        try{
            return getMapper().update( nameNodeIp ) == 1;
        }finally {
            BDMSMyBatisUtil.closeSqlSession();
        }
    }

    private NameNodeIpMapper getMapper(){
        return BDMSMyBatisUtil.getMapper(NameNodeIpMapper.class);
    }
}
