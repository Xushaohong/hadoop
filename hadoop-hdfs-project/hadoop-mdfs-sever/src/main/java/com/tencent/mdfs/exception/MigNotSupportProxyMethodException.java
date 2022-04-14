package com.tencent.mdfs.exception;

/**
 * create:chunxiaoli
 * Date:2/8/18
 */
public class MigNotSupportProxyMethodException  extends MigHadoopException{

    public MigNotSupportProxyMethodException(String m) {
        super("not support method in nn proxy :"+m);
    }
}
