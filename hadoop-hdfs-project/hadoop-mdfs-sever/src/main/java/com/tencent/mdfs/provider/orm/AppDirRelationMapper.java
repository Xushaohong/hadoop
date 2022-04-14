package com.tencent.mdfs.provider.orm;

import com.tencent.mdfs.provider.pojo.AppDirRelation;
import java.util.List;

public interface AppDirRelationMapper {
    Integer insert(AppDirRelation record);

    Integer insertSelective(AppDirRelation record);

    AppDirRelation selectByPrimaryKey(Integer id);

    Integer updateByPrimaryKeySelective(AppDirRelation record);

    Integer updateByPrimaryKey(AppDirRelation record);

    List<AppDirRelation> selectAll();
}