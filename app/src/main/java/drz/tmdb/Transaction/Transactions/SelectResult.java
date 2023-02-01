package drz.tmdb.Transaction.Transactions;

import drz.tmdb.Memory.TupleList;

public class SelectResult {
    TupleList tpl;
    String[] className;
    String[] attrname;
    int[] attrid;
    String[] type;

    public SelectResult(TupleList tpl, String[] className,String[] attrname, int[] attrid, String[] type) {
        this.className=className;
        this.tpl = tpl;
        this.attrname = attrname;
        this.attrid = attrid;
        this.type = type;
    }

    public SelectResult(){}

    public String[] getClassName() {
        return className;
    }

    public void setClassName(String[] className) {
        this.className = className;
    }

    public TupleList getTpl() {
        return tpl;
    }

    public void setTpl(TupleList tpl) {
        this.tpl = tpl;
    }

    public String[] getAttrname() {
        return attrname;
    }

    public void setAttrname(String[] attrname) {
        this.attrname = attrname;
    }

    public int[] getAttrid() {
        return attrid;
    }

    public void setAttrid(int[] attrid) {
        this.attrid = attrid;
    }

    public String[] getType() {
        return type;
    }

    public void setType(String[] type) {
        this.type = type;
    }
}
