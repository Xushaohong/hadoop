package com.tencent.mdfs.exception;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class MigHadoopException extends RuntimeException{

    private String className;

    public MigHadoopException(String s) {
        super(s);
    }

    public MigHadoopException(String s,String className) {
        super(s);
        this.className=className;
    }
}
