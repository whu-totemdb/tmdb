package edu.whu.tmdb.storage.memory.SystemTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ObjectTable implements Serializable {
    public List<ObjectTableItem> objectTableList = new ArrayList<>();
    public int maxTupleId = 0;
    public void clear(){
        objectTableList.clear();
        maxTupleId = 0;
    }

    public int getClassIdByTupleId(int tupleId){
        for(ObjectTableItem item : this.objectTableList){
            if(item.tupleid == tupleId)
                return item.classid;
        }
        return -1;
    }
}

