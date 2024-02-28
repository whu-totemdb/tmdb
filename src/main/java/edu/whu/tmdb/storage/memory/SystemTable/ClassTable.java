package edu.whu.tmdb.storage.memory.SystemTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ClassTable implements Serializable {
    public List<ClassTableItem> classTableList = new ArrayList<>();
    public int maxid = 0;

    public void clear(){
        classTableList.clear();
        maxid = 0;
    }
}

