package com.tencent.mdfs.util;

import java.io.File;
import java.util.List;

/**
 * create:chunxiaoli
 * Date:2/8/18
 */
public class PathUtil {

    public static String getRootDir(String file){
        if(StringUtil.isEmpty(file)){
            return null;
        }

        if(file.equals("/")){
            return file;
        }

        String [] parts=file.split(File.separator);

        return File.separator+(file.startsWith(File.separator)?parts[1]:parts[0]);
    }

    public static String getRootDir(String file,String parts[]){
        if(StringUtil.isEmpty(file)){
            return null;
        }

        if(file.equals("/")){
            return file;
        }
        return File.separator+(file.startsWith(File.separator)?parts[1]:parts[0]);
    }

    public static String normalize(String realPath, String path) {
        if(StringUtil.isEmpty(path)){
            return null;
        }

        if(StringUtil.isEmpty(realPath)){
            return null;
        }
        String root=getRootDir(path);

        if(StringUtil.isEmpty(root)){
            return null;
        }
        return (realPath+File.separator+path.replace(root,"")).replaceAll("//","/");
    }

    //todo
    public static String join(List<String> list) {
        StringBuilder sb=new StringBuilder();
        int len=list.size();
        for(String s:list){
            sb.append(s).append(len-->1?File.separator:"");
        }
        return sb.toString();
    }
}
