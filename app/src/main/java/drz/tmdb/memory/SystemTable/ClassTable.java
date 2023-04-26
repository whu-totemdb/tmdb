package drz.tmdb.memory.SystemTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ClassTable implements Serializable {
    public List<ClassTableItem> classTable=new ArrayList<>();
    public int maxid=0;

    public void clear(){
        classTable.clear();
        maxid = 0;
    }
}

