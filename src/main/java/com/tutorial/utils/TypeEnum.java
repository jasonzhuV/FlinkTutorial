package com.tutorial.utils;

/**
 * @author weijinglun
 * @date 2021.10.18
 */
public enum TypeEnum {
    // /**
    // * 插入
    // */
    // public final static String CREATE_OP = "+I";
    // /**
    // * 查询
    // */
    // public final static String READ_OP = "-U";
    // /**
    // * 更新
    // */
    // public final static String UPDATE_OP = "+U";
    // /**
    // * 删除
    // */
    // public final static String DELETE_OP = "-D";

    INSERT("+I"), UPDATE("+U"), READ("-U"), DELETE("-D");

    String name;

    TypeEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
