package com.tencent.mdfs.util;

import java.io.UnsupportedEncodingException;

/**
 * create:chunxiaoli
 * Date:2/27/18
 */
public class StringUtil {
    public static byte[] bytes(String s) {
        if (isEmpty(s)) {
            return null;
        }
        try {

            return s.getBytes("utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String string(byte[] data) {
        if (data==null) {
            return null;
        }
        try {

            return new String(data,"utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean isEmpty(String s) {
        return s == null || s.equals("");
    }
}
