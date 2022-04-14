package com.tencent.mdfs.provider;

import com.tencent.mdfs.provider.dto.AppDirRelationDto;
import java.util.List;

/**
 * vnnconfig RPC interface */
public interface AppDirRelationService {
  Integer appDirRelationInsert(AppDirRelationDto arg0);

  Integer appDirRelationInsertSelective(AppDirRelationDto arg0);

  AppDirRelationDto appDirRelationSelectByPrimaryKey(Integer arg0);

  Integer appDirRelationUpdateByPrimaryKey(AppDirRelationDto arg0);

  Integer appDirRelationUpdateByPrimaryKeySelective(AppDirRelationDto arg0);

  List<AppDirRelationDto> selectAll();
}
