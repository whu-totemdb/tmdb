package drz.tmdb.Transaction.SystemTable;

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
}

