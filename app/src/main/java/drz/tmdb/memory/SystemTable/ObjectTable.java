package drz.tmdb.memory.SystemTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ObjectTable implements Serializable {
    public  List<ObjectTableItem> objectTable =new ArrayList<>();
    public int maxTupleId = 0;
    public void clear(){
       objectTable.clear();
        maxTupleId = 0;
    }

    public int getClassIdByTupleId(int tupleId){
        for(ObjectTableItem item : this.objectTable){
            if(item.tupleid == tupleId)
                return item.classid;
        }
        return -1;
    }
}

