package com.tencent.mdfs.exception;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class MigPermissionDenyException extends MigNotRetryableException{


    public MigPermissionDenyException(String msg) {
        super(msg);
    }
}
