package edu.whu.tmdb.storage.memory.SystemTable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class DeputyTableItem implements Serializable {
    public int originid = 0;           // 源类id(从哪里代理而来)
    public int deputyid = 0;           // 代理类id
    public String[] deputyrule = new String[0];    // 代理规则


    public DeputyTableItem() {}

    public DeputyTableItem(int originid, int deputyid, String[] deputyrule) {
        this.originid = originid;           // 源类id
        this.deputyid = deputyid;           // 代理类id
        this.deputyrule = deputyrule;       // 代理规则
    }

    /**
     * 给定对象，判断是否与此代理对象相等
     */
    @Override
    public boolean equals(Object object){
        if (this == object) { return true; }
        if (!(object instanceof DeputyTableItem)) {
            return false;
        }
        DeputyTableItem oi = (DeputyTableItem) object;
        if(this.originid != oi.originid){
            return false;
        }
        if(this.deputyid != oi.deputyid){
            return false;
        }
        if(this.deputyrule != oi.deputyrule){
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Objects.hash(this.originid) + Objects.hash(Arrays.stream(this.deputyrule).toArray()) + Objects.hash(this.deputyid);
        return result;
    }
}
