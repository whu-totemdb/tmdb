package drz.tmdb.memory.SystemTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DeputyTable implements Serializable {
    public List<DeputyTableItem> deputyTable=new ArrayList<>();

    public void clear(){
        deputyTable.clear();
    }
}

