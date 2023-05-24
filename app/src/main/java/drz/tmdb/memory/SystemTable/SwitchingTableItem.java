package drz.tmdb.memory.SystemTable;

import java.io.Serializable;
import java.util.Objects;


public class SwitchingTableItem implements Serializable {
    public int oriId;
    public int oriAttrid;
    public String oriAttr;
    public int deputyId;
    public int deputyAttrId;
    public String deputyAttr;
    public String rule = "";

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
        if(this.rule!=oi.rule){
            return false;
        }
        if(this.oriAttr!=oi.oriAttr){
            return false;
        }
        if(this.deputyAttr!=oi.deputyAttr){
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
