package com.tencent.mdfs.util;


import org.apache.commons.codec.digest.DigestUtils;


/**
 * create:chunxiaoli
 * Date:2/27/18
 */
public class Md5Util {
    public static String md5( String key,String salt ){
        if(key == null ){
            return  key;
        }
        return DigestUtils.md5Hex( key + salt );
    }

    public static void main(String[] args) {
        System.out.println( md5("100-5","123123"));
    }
    /*public static String generate(String password) {
//        生成随机盐，长度12位
        byte[] bytes = new byte[12];
        SecureRandom random = new SecureRandom();
        random.nextBytes(bytes);

        StringBuilder builder = new StringBuilder();
//        将字节数组变为字符串
        for (int i = 0; i < bytes.length; i++) {
//            将生成的值，全部映射到0-255 之间
            int val = ((int) bytes[i]) & 0xff;
            if (val < 16) {
//                为了控制盐的长度，这里小于16 的值，我们将它补为 大于16的值；
//                这样，生的盐的长度是固定的：bytes * 2 ;
                builder.append(Integer.toHexString(val + 16));
            } else {
                builder.append(Integer.toHexString(val));
            }
        }
//        最终的盐，长度是 12*2 = 24 ；
        String salt = builder.toString();
//        先加盐Md5一把，再将 MD5 转换成 24位的 base64 位编码
        password = md5Hex(password + salt);
        char[] cs = new char[salt.length() + password.length()];
        for (int i = 0; i < cs.length; i += 4) {
//            密码编码
            cs[i] = password.charAt(i / 2);
            cs[i + 2] = password.charAt(i / 2 + 1);
//            盐编码
            cs[i + 1] = salt.charAt(i / 2);
            cs[i + 3] = salt.charAt(i / 2 + 1);

        }
        return new String(cs);
    }

    *//**
     * 获取十六进制字符串形式的MD5摘要
     *//*
    private static String md5Hex(String src) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bs = md5.digest(src.getBytes());
            return new String(new BASE64Encoder().encode(bs));
        } catch (Exception e) {
            return null;
        }
    }*/
}
