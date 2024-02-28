package edu.whu.tmdb.query.operations.utils;/*
 * className:ClassType
 * Package:edu.whu.tmdb.Transaction.Transactions.utils
 * Description:
 * @Author: xyl
 * @Create:2023/9/10 - 15:44
 * @Version:v1
 */

public enum ClassType {
    ORI(1),  // 1,表示源类
    DEP(2); // 2，表示代理类

    private final int value;

    ClassType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}

