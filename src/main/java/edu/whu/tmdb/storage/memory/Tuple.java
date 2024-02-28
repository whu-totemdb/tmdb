package edu.whu.tmdb.storage.memory;


import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class Tuple implements Serializable {
    public int tupleSize;       // tuple中字段的数量
    public int tupleId;
    public int classId;         // tuple所属的class id
    public int[] tupleIds;      // tuple id填满的数组
    public Object[] tuple;      // tuple中字段列表（元数据）
    public boolean delete;      // 删除位

    public Tuple(Object[] values) {
        tuple = values.clone();
        tupleSize = values.length;
    }

    public int getTupleId() { return tupleId; }

    public void setTupleId(int tupleId) { this.tupleId = tupleId; }

    // 根据给定变量对tuple进行赋值
    public void setTuple(int header, int tupleid, int classid, Object[] tuple) {
        this.tupleSize = header;
        this.tupleId = tupleid;
        this.classId = classid;
        this.tuple = tuple;
        int[] ids = new int[this.tupleSize];
        Arrays.fill(ids, tupleid);
        this.tupleIds = ids;
    }

    public Tuple(){}

    @Override
    public boolean equals(Object object){
        if (this == object) {
            return true;
        }
        if (!(object instanceof Tuple)) {
            return false;
        }
        Tuple tuple = (Tuple) object;
        if (tuple.tuple.length != this.tuple.length) {
            return false;
        }
        for (int i = 0; i < tuple.tuple.length; i++){
            if (!this.tuple[i].equals(tuple.tuple[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Objects.hash(this.tupleSize);
        for (Object o : this.tuple) {
            result = 31 * result + Objects.hash(o);
        }
        return result;
    }
}
