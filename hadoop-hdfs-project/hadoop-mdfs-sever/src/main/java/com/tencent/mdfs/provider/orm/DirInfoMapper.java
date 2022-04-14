package com.tencent.mdfs.provider.orm;

import com.tencent.mdfs.provider.pojo.DirInfo;
import java.util.List;

public interface DirInfoMapper {
    Integer insert(DirInfo record);
    Integer batchInsert(List<DirInfo> list);

    Integer insertSelective(DirInfo record);

    DirInfo selectByPrimaryKey(Integer id);

    Integer updateByPrimaryKeySelective(DirInfo record);

    Integer updateByPrimaryKey(DirInfo record);

    List<DirInfo> selectAll();

    DirInfo dirInfoSelectByConditions(DirInfo pojo);
}