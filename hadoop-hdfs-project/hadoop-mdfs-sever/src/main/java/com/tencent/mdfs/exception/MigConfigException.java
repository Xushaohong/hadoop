package com.tencent.mdfs.exception;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class MigConfigException extends MigHadoopException{
    public MigConfigException(String s) {
        super(" config error :"+s);
    }
}
