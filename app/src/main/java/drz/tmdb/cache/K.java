package drz.tmdb.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import drz.tmdb.level.Constant;

public class K implements Serializable, Comparable{

    // 表示方式1，非空
    public String keyString;

    // 表示方式2
    public byte[] keyBytes;

    public K(){
    }

    public K(String str){
        if(str.length() < Constant.MAX_KEY_LENGTH){
            // 将str扩展到MAX_KEY_LENGTH位
            byte[] ret = new byte[Constant.MAX_KEY_LENGTH];
            byte[] temp = str.getBytes();
            for(int i=0;i<temp.length;i++){
                ret[i]=temp[i];
            }
            for(int i=temp.length;i<Constant.MAX_KEY_LENGTH;i++){
                // 不足的地方补全0
                ret[i]=(byte)32;
            }
            this.keyBytes = ret;
            this.keyString = new String(ret);
        }else{
            this.keyString = str;
        }

    }

    public K(byte[] bytes){
        this.keyString = new String(bytes);
    }

    public byte[] serialize(){
        if(this.keyString != null){
            // 如果有keyBytes则直接返回，否则通过string构造
            if(this.keyBytes == null)
                this.keyBytes = this.keyString.getBytes();
            return this.keyString.getBytes();
        }
        else return new byte[0];
    }


    @Override
    public String toString(){
        return this.keyString;
    }


    @Override
    public int compareTo(Object o) {
        if(o == null)
            return 0;
        if(o instanceof K){
            return this.keyString.compareTo(((K) o).keyString);
        }
        return 0;
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
        return this.keyString.equals(((K)obj).keyString);
    }

    @Override
    public int hashCode(){
        return this.keyString.hashCode();
    }


}
