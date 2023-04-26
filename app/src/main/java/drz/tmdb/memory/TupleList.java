package drz.tmdb.memory;
import java.io.Serializable;
import java.util.*;
public class TupleList implements Serializable {
    public List<Tuple> tuplelist = new ArrayList<Tuple>();
    public int tuplenum = 0;

    public void addTuple(Tuple tuple){
        this.tuplelist.add(tuple);
        tuplenum++;
    }

}
