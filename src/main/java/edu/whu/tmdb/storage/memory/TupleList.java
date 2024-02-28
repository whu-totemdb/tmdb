package edu.whu.tmdb.storage.memory;

import java.io.Serializable;
import java.util.*;

public class TupleList implements Serializable {
    public List<Tuple> tuplelist = new ArrayList<Tuple>();
    public int tuplenum = 0;

    public TupleList() {}

    public TupleList(int tupleSize, int tupleListSize) {
        for (int i = 0; i < tupleListSize; i++){
            Tuple tuple = new Tuple();
            int[] temp = new int[tupleSize];
            Arrays.fill(temp,-1);
            tuple.tupleIds = temp;
            tuple.tuple = new Object[tupleSize];
            this.addTuple(tuple);
        }
    }

    public void addTuple(Tuple tuple) {
        this.tuplelist.add(tuple);
        tuplenum++;
    }

}
