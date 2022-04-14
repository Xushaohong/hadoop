package com.tencent.mdfs.exception;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class MigHadoopServerException extends MigHadoopException{

    private String className;

    public MigHadoopServerException(String s) {
        super(s);
    }

    public MigHadoopServerException(String s, String className) {
        super(s);
        this.className=className;
    }
}
