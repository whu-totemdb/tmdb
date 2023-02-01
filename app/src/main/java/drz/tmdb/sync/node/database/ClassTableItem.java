package drz.tmdb.sync.node.database;

import java.io.Serializable;

public class ClassTableItem implements Serializable {
    public String classname = null;        //类名
    public int classid = 0;                //类id
    public int attrnum = 0;                //类属性个数
    public int    attrid = 0;
    public String attrname = null;         //属性名
    public String attrtype = null;         //属性类型
    public String classtype = null;

    public ClassTableItem(String classname, int classid, int attrnum,int attrid, String attrname, String attrtype,String classtype) {
        this.classname = classname;
        this.classid = classid;
        this.attrnum = attrnum;
        this.attrname = attrname;
        this.attrtype = attrtype;
        this.attrid = attrid;
        this.classtype = classtype;
    }
    public ClassTableItem(){}
}
