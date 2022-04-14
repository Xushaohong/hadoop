package com.tencent.mdfs.provider;

import com.tencent.mdfs.provider.dto.DirInfoDto;
import java.util.List;

/**
 * vnnconfig RPC interface */
public interface DirInfoService {
  Integer dirInfoInsert(DirInfoDto arg0);
  Integer dirInfoBatchInsert(List<DirInfoDto> arg0);

  Integer dirInfoInsertSelective(DirInfoDto arg0);

  DirInfoDto dirInfoSelectByPrimaryKey(Integer arg0);

  DirInfoDto dirInfoSelectByConditions(DirInfoDto arg0);

  Integer dirInfoUpdateByPrimaryKey(DirInfoDto arg0);

  Integer dirInfoUpdateByPrimaryKeySelective(DirInfoDto arg0);

  List<DirInfoDto> selectAll();
}
