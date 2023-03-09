package drz.tmdb.Memory.SystemTable;

import java.io.Serializable;

import drz.tmdb.Memory.Tuple;

public class ObjectTableItem{


    public int classid = 0;    //类id
    public int tupleid = 0;    //元组id
    public int sstSuffix = -1;  // SSTable文件的后缀

    public ObjectTableItem() {
    }

    public ObjectTableItem( int classid, int tupleid) {
        this.classid = classid;
        this.tupleid = tupleid;
    }

}