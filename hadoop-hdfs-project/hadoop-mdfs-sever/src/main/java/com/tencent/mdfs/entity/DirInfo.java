package com.tencent.mdfs.entity;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class DirInfo {
    private String name;
    private int permission;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPermission() {
        return permission;
    }

    public void setPermission(int permission) {
        this.permission = permission;
    }

    public boolean validOp(OpType opType) {
        if(opType==null){
            return false;
        }
        return opType.getValue()<=permission;

    }

    @Override
    public String toString() {
        return "DirInfo{" +
                "name='" + name + '\'' +
                ", permission=" + permission +
                '}';
    }
}
