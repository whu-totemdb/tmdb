package drz.tmdb.Memory.SystemTable;

import java.io.Serializable;

public class ObjectTableItem{


    public int classid = 0;    //类id
    public int tupleid = 0;    //元组id
    public int sstSuffix = -1;  // SSTable文件的后缀
    public boolean delete = false; // 删除位


    public ObjectTableItem() {
    }

    public ObjectTableItem( int classid, int tupleid, boolean delete) {
        this.classid = classid;
        this.tupleid = tupleid;
        this.delete = delete;
    }

}