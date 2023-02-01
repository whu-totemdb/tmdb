package drz.tmdb.sync.node.database;

import java.io.Serializable;

public class ObjectTableItem implements Serializable {
    public int classid = 0;    //类id
    public int tupleid = 0;    //元组id
    public int blockid = 0;    //块id
    public int offset = 0;      //元祖偏移量

    public String data;



    public ObjectTableItem( int classid, int tupleid, int blockid, int offset) {
        this.classid = classid;
        this.tupleid = tupleid;
        this.blockid = blockid;
        this.offset = offset;
    }




    public ObjectTableItem() {
    }
}
