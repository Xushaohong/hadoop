package com.tencent.mdfs.util;

/**
 * create:chunxiaoli
 * Date:6/5/18
 */
public class RouteInfoUtil {
    public static boolean isRoot( String path ){
        if( path == null || path.length() == 0){
            return true;
        }
        return "/".equals( path );
    }

    public static void main(String[] args) {
        System.out.println( isRoot("") );
    }

}
