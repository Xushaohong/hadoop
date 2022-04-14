package com.tencent.mdfs.provider.orm;

import com.tencent.mdfs.provider.pojo.AppInfo;
import java.util.List;

public interface AppInfoMapper {
    Integer insert(AppInfo record);

    Integer insertSelective(AppInfo record);

    AppInfo selectByPrimaryKey(Integer id);

    Integer updateByPrimaryKeySelective(AppInfo record);

    Integer updateByPrimaryKey(AppInfo record);

    Integer updateByAppId(AppInfo record);

    List<AppInfo> selectAll();

    List<AppInfo> selectByPrimaryKeyList(List<Integer> arg0);
}