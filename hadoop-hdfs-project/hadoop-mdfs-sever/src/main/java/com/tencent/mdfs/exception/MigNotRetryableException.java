package com.tencent.mdfs.exception;

public class MigNotRetryableException extends RuntimeException{
    private Exception target;
    public MigNotRetryableException(String s) {
        super(s);
    }
    public MigNotRetryableException( Exception target ){
        this.target = target;
    }

    public Exception getTarget() {
        return target;
    }

    public void setTarget(Exception target) {
        this.target = target;
    }
}
