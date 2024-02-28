package edu.whu.tmdb.storage.memory.SystemTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DeputyTable implements Serializable {
    public List<DeputyTableItem> deputyTableList = new ArrayList<>();

    public void clear(){
        deputyTableList.clear();
    }
}

