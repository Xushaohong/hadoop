package com.tencent.mdfs.entity;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class OpAction {
    public final boolean  adminOperator;
    public final String dir;
    public final  OpType opType;

    public OpAction(String dir, OpType opType) {
        adminOperator = false;
        this.dir = dir;
        this.opType = opType;
    }

    public OpAction(String dir, OpType opType,Boolean adminOperator) {
        this.adminOperator = adminOperator;
        this.dir = dir;
        this.opType = opType;
    }

    @Override
    public String toString() {
        return "OpAction{" +
                "dir='" + dir + '\'' +
                ", opType=" + opType +
                '}';
    }
}
