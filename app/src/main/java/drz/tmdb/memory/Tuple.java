package drz.tmdb.memory;


import java.io.Serializable;
import java.util.Objects;

public class Tuple implements Serializable {
    public int tupleHeader;
    public int tupleId;
    public int classId;
    public int[] tupleIds;
    public Object[] tuple;
    public boolean delete; // 删除位

    public Tuple(Object[] values) {
        tuple = values.clone();
        tupleHeader = values.length;
    }

    public int getTupleId() {
        return tupleId;
    }

    public void setTupleId(int tupleId) {
        this.tupleId = tupleId;
    }

    public Tuple(){}

    @Override
    public boolean equals(Object object){
        if(this==object) return true;
        if (!(object instanceof Tuple)) {
            return false;
        }
        Tuple tuple=(Tuple) object;
        if(tuple.tuple.length!=this.tuple.length) return false;
        for(int i=0;i<tuple.tuple.length;i++){
            if(!this.tuple[i].equals(tuple.tuple[i])) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Objects.hash(this.tupleHeader);
        for(int i=0;i<this.tuple.length;i++){
            result = 31 * result + Objects.hash(this.tuple[i]);
        }
        return result;
    }
}
