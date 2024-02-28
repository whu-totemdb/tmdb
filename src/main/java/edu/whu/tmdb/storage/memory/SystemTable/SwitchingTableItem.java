package edu.whu.tmdb.storage.memory.SystemTable;

import java.io.Serializable;
import java.util.Objects;


public class SwitchingTableItem implements Serializable {
    public int oriId;           // 源类id
    public int oriAttrid;       // 源类属性id
    public String oriAttr;      // 源类属性名称
    public int deputyId;        // 代理类id
    public int deputyAttrId;    // 代理类属性id
    public String deputyAttr;   // 代理类属性名称
    public String rule = "";    // 代理规则

    public SwitchingTableItem(int oriId, int oriAttrid, String oriAttr, int deputyId, int deputyAttrId, String deputyAttr, String rule) {
        this.oriId = oriId;
        this.oriAttrid = oriAttrid;
        this.oriAttr = oriAttr;
        this.deputyId = deputyId;
        this.deputyAttrId = deputyAttrId;
        this.deputyAttr = deputyAttr;
        this.rule = rule;
    }

    public SwitchingTableItem(){}

    @Override
    public boolean equals(Object object){
        if(this==object) return true;
        if (!(object instanceof SwitchingTableItem)) {
            return false;
        }
        SwitchingTableItem oi=(SwitchingTableItem) object;
        if(this.oriId!=oi.oriId){
            return false;
        }
        if(this.oriAttrid!=oi.oriAttrid){
            return false;
        }
        if(this.deputyId!=oi.deputyId){
            return false;
        }
        if(this.deputyAttrId!=oi.deputyAttrId){
            return false;
        }
        if(!Objects.equals(this.rule, oi.rule)){
            return false;
        }
        if(!Objects.equals(this.oriAttr, oi.oriAttr)){
            return false;
        }
        if(!Objects.equals(this.deputyAttr, oi.deputyAttr)){
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Objects.hash(this.oriId)
                +Objects.hash(this.oriAttrid)
                +Objects.hash(this.oriAttr)
                +Objects.hash(this.deputyId)
                +Objects.hash(this.deputyAttrId)
                +Objects.hash(this.deputyAttr)
                +Objects.hash(this.rule)
        ;
        return result;
    }
}
