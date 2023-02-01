package drz.tmdb.Memory;


import java.io.Serializable;

public class Tuple implements Serializable {
    public int tupleHeader;
    public Object[] tuple;

    public Tuple(Object[] values) {
        tuple = values.clone();
        tupleHeader = values.length;
    }

    public Tuple(){}

}
