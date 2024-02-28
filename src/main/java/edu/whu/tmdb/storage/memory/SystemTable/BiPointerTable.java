package edu.whu.tmdb.storage.memory.SystemTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BiPointerTable implements Serializable {
    public List<BiPointerTableItem> biPointerTableList = new ArrayList<>();
}
