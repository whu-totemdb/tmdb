package edu.whu.tmdb.query.operations.utils;

import edu.whu.tmdb.storage.memory.TupleList;

import java.util.Arrays;

public class SelectResult {
    // 数据信息
    TupleList tpl;          // 元组数据列表

    // 表头信息，相同下标为一列
    String[] className;     // 字段所属的类名
    String[] attrname;      // 字段名
    String[] alias;         // 字段的别名，在进行select时会用到
    int[] attrid;           // 显示时使用
    String[] type;          // 字段数据类型(char, int)

    public SelectResult(TupleList tpl, String[] className, String[] attrname, String[] alias, int[] attrid, String[] type) {
        this.tpl = tpl;
        this.className = className;
        this.attrname = attrname;
        this.alias = alias;
        this.attrid = attrid;
        this.type = type;
    }

    /**
     * 初始化表头大小，设置selectResult表项的数量
     * @param size selectResult表项的数量
     */
    public SelectResult(int size) {
        setClassName(new String[size]);
        setAttrname(new String[size]);
        setAttrid(new int[size]);
        setType(new String[size]);
        setAlias(new String[size]);
    }

    public SelectResult(){}

    // 读写元组数据
    public void setTpl(TupleList tpl) { this.tpl = tpl; }

    public TupleList getTpl() { return tpl; }

    // 读写字段所属类名
    public void setClassName(String[] className) { this.className = className; }

    public String[] getClassName() { return className; }

    // 读写字段名
    public void setAttrname(String[] attrname) { this.attrname = attrname; }

    public String[] getAttrname() { return attrname; }

    // 读写字段别名
    public void setAlias(String[] alias) { this.alias = alias; }

    public String[] getAlias() { return alias; }

    // 读写字段id（我也不知道有什么用
    public void setAttrid(int[] attrid) { this.attrid = attrid; }

    public int[] getAttrid() { return attrid; }

    // 读写字段属性
    public void setType(String[] type) { this.type = type; }

    public String[] getType() { return type; }
}
