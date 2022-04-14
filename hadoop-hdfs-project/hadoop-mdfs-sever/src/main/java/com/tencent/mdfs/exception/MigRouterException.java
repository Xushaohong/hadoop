package com.tencent.mdfs.exception;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class MigRouterException extends MigHadoopException{

    public MigRouterException(String src) {
        super("can not find route info for "+src);
    }
}
