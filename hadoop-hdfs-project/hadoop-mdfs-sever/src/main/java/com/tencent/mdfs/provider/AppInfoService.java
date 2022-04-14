package com.tencent.mdfs.provider;

import com.tencent.mdfs.provider.dto.AppInfoDto;
import java.util.List;

/**
 * vnnconfig RPC interface */
public interface AppInfoService {
  Integer appInfoInsert(AppInfoDto arg0);

  Integer appInfoInsertSelective(AppInfoDto arg0);

  AppInfoDto appInfoSelectByPrimaryKey(Integer arg0);

  Integer appInfoUpdateByPrimaryKey(AppInfoDto arg0);

  Integer appInfoUpdateByPrimaryKeySelective(AppInfoDto arg0);

  List<AppInfoDto> selectAll();

}
