package com.tencent.mdfs.storage;

import com.tencent.mdfs.provider.pojo.AppDirRelation;
import com.tencent.mdfs.provider.pojo.AppInfo;
import com.tencent.mdfs.provider.pojo.AppRelation;
import com.tencent.mdfs.provider.pojo.DirInfo;
import com.tencent.mdfs.provider.util.DaoUtil;
import com.tencent.mdfs.util.VnnConfigUtil;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class StorageUtil {




    public static List<String> readRemoteLines(String file) {
        try {

            InputStream inputStream = VnnConfigUtil.loadInputStreamFromRemoteOrLocal(file);

            if(inputStream!=null){
                return IOUtils.readLines(inputStream, "utf-8");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<String> readServerOrAppConfig(String file) {
        try {

            InputStream inputStream = VnnConfigUtil.loadInputStreamFromRemoteOrLocal(file);

            if(inputStream!=null){
                return IOUtils.readLines(inputStream, "utf-8");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }



    public static List<AppInfo> loadAppInfos() {
        return DaoUtil.queryAllApps();
    }


    public static List<DirInfo> loadDirInfos() {
        return DaoUtil.queryAllDirs();
    }


    public static List<AppDirRelation> loadAppDirRelations() {
        return DaoUtil.queryAllAppDirs();
    }

    public static List<AppRelation> loadAppRelations() {
        return DaoUtil.queryAllAppRelations();
    }
}
