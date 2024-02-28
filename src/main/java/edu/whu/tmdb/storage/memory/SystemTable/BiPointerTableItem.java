package edu.whu.tmdb.storage.memory.SystemTable;

import java.io.Serializable;
import java.util.Objects;

public class BiPointerTableItem implements Serializable {
    public int classid = 0;         // 源类id
    public int objectid = 0;        // 源类对象id
    public int deputyid = 0;        // 代理类id
    public int deputyobjectid = 0;  // 代理类对象id

    public BiPointerTableItem(int classid, int objectid, int deputyid, int deputyobjectid) {
        this.classid = classid;
        this.objectid = objectid;
        this.deputyid = deputyid;
        this.deputyobjectid = deputyobjectid;
    }

    public BiPointerTableItem(){}

    @Override
    public boolean equals(Object object){
        if(this==object) return true;
        if (!(object instanceof BiPointerTableItem)) {
            return false;
        }
        BiPointerTableItem oi=(BiPointerTableItem) object;
        if(this.classid!=oi.classid){
            return false;
        }
        if(this.objectid!=oi.objectid){
            return false;
        }
        if(this.deputyid!=oi.deputyid){
            return false;
        }
        if(this.deputyobjectid!=oi.deputyobjectid){
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Objects.hash(this.classid)
                +Objects.hash(this.objectid)
                +Objects.hash(this.deputyid)
                +Objects.hash(this.deputyobjectid)
        ;
        return result;
    }

}
