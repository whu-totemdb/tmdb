package edu.whu.tmdb.storage.memory.SystemTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SwitchingTable implements Serializable {
    public List<SwitchingTableItem> switchingTableList = new ArrayList<>();

}
