package edu.whu.tmdb.query.operations.utils;/*
 * className:DeputyClassType
 * Package:edu.whu.tmdb.Transaction.Transactions.utils
 * Description:
 * @Author: xyl
 * @Create:2023/9/10 - 15:21
 * @Version:v1
 */

public enum DeputyClassType {
    SELECT(1),  // 代理类1，表示select代理类
    GROUPBY(2); // 代理类2，表示groupby代理类

    private final int value;

    DeputyClassType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
