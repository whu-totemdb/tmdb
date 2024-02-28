package edu.whu.tmdb.storage.utils;

import scala.Char;

import java.io.Serializable;
import java.util.Objects;

public class K implements Serializable, Comparable{

    public String key = "";

    public K() {}

    /**
     * 构造函数，将输入的键值key标准化
     * @param key 输入的键值
     */
    public K(String key){
        // key最大长度为Constant.MAX_KEY_LENGTH，不足的补0
        if (key.length() < Constant.MAX_KEY_LENGTH) {
            this.key = key + new String(new byte[Constant.MAX_KEY_LENGTH - key.length()]);
        }
        else {
            this.key = key;
        }
    }

    public K(byte[] bytes){
        // key最大长度为Constant.MAX_KEY_LENGTH，不足的补0
        if(bytes.length < Constant.MAX_KEY_LENGTH){
            this.key = new String(bytes) + new String(new byte[Constant.MAX_KEY_LENGTH - bytes.length]);
        }
        else
            this.key = new String(bytes);
    }

    public byte[] serialize(){
        return this.key.getBytes();
    }

    @Override
    public int compareTo(Object o) {
        if(o == null)
            return 0;
        if(o instanceof K){
            return this.key.compareTo(((K) o).key);
        }
        return 0;
    }

    @Override
    public String toString(){
        return this.key;
    }

    @Override
    public boolean equals(Object obj){
        // 如果是同一个对象，直接返回true
        if (this == obj) {
            return true;
        }

        // 如果obj为null或者不是同一个类的实例，返回false
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        // 自定义相等性比较规则: 比较string
        return this.key.equals(((K) obj).key);
    }

    @Override
    public int hashCode(){
        return this.key.hashCode();
    }


}
