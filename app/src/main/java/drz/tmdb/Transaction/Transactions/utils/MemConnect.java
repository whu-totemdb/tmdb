package drz.tmdb.Transaction.Transactions.utils;

import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import drz.tmdb.memory.MemManager;
import drz.tmdb.memory.Tuple;
import drz.tmdb.memory.SystemTable.BiPointerTable;
import drz.tmdb.memory.SystemTable.BiPointerTableItem;
import drz.tmdb.memory.SystemTable.ClassTable;
import drz.tmdb.memory.SystemTable.ClassTableItem;
import drz.tmdb.memory.SystemTable.DeputyTable;
import drz.tmdb.memory.SystemTable.ObjectTable;
import drz.tmdb.memory.SystemTable.ObjectTableItem;
import drz.tmdb.memory.SystemTable.SwitchingTable;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;

public class MemConnect {
    //进行内存操作的一些一些方法和数据
    private MemManager mem;

    public static ObjectTable topt;
    private static ClassTable classt;
    private static DeputyTable deputyt;
    private static BiPointerTable biPointerT;
    private static SwitchingTable switchingT;

//    private static ObjectTable topt=mem.objectTable;
//    private static ClassTable classt = mem.classTable;
//    private static DeputyTable deputyt = mem.deputyTable;
//    private static BiPointerTable biPointerT = mem.biPointerTable;
//    private static SwitchingTable switchingT = mem.switchingTable;

    public MemConnect() {
    }

    public MemConnect(MemManager mem) {
        this.mem = mem;
        this.topt=mem.objectTable;
        this.classt = mem.classTable;
        this.deputyt = mem.deputyTable;
        this.biPointerT = mem.biPointerTable;
        this.switchingT = mem.switchingTable;
    }

    //    public MemConnect(MemManager memManager){
//        this.mem = memManager;
//        classt.classTable = (List<ClassTableItem>) mem.getClassTable();
//        deputyt.deputyTable = (List<DeputyTableItem>) mem.getDeputyTable();
//        biPointerT.biPointerTable = (List<BiPointerTableItem>) mem.getBiPointerTable();
//        switchingT.switchingTable = (List<SwitchingTableItem>) mem.getSwitchingTable();
//
//    };

    //获取tuple
    public Tuple GetTuple(int id) {
        Object searchResult = this.mem.search("t" + id);
        Tuple t = null;
        if(searchResult == null)
            return null;
        if(searchResult instanceof Tuple)
            t = (Tuple) searchResult;
        else if(searchResult instanceof String)
            return JSON.parseObject((String) searchResult, Tuple.class);
        if(t.delete)
            return null;
        else
            return t;
    }

    //插入tuple
    public void InsertTuple(Tuple tuple){
        this.mem.add(tuple);
    }

    //删除tuple
    public void DeleteTuple(int id){
        if(id>=0){
            Tuple tuple = new Tuple();
            tuple.tupleId = id;
            tuple.delete = true;
            mem.add(tuple);
        }
    }

    //更新tuple
    public void UpateTuple(Tuple tuple,int tupleId){
        tuple.tupleId = tupleId;
        this.mem.add(tuple);
    }

    //获取表的属性元素
//    public ArrayList<ClassTableItem> getSelectItem(FromItem fromItem){
//        ArrayList<ClassTableItem> elicitAttrItemList=new ArrayList<>();
//        for(ClassTableItem item : classt.classTable){
//            //如果classTableItem中的className对上了fromItem就加入结果中
//            if(item.classname.equals(((Table)fromItem).getName())){
//                //硬拷贝，不然后续操作会影响原始信息。
//                ClassTableItem temp=item.getCopy();
//                //因为后续有许多针对alias的比对操作，所以，如果fromItem中使用了alias，则在classTableItem中的alias属性中存入该值
//                if(fromItem.getAlias()!=null) temp.alias=fromItem.getAlias().getName();
//                elicitAttrItemList.add(temp);
//            }
//        }
//        return elicitAttrItemList;
//    }
//
//    public ArrayList<ClassTableItem> getSelectItem(FromItem fromItem, List<Column> columnList){
//        // 从class表中提取将要获取的元素。
//        ArrayList<ClassTableItem> elicitAttrItemList=new ArrayList<>();
//        for(ClassTableItem item : classt.classTable){
//            if(item.classname.equals(fromItem.toString())){
//                String attrName=item.attrname;
//                boolean flag=false;
//                for(Column column:columnList){
//                    Column c=(Column) column;
//                    if(c.getTable()!=null && !(c.getTable().equals(fromItem.toString())&& c.getTable().equals(fromItem.getAlias().getName()))) continue;
//                    if(attrName.equals(c.getColumnName())) {
//                        flag=true;
//                        break;
//                    }
//                }
//                if(flag) elicitAttrItemList.add(item);
//            }
//        }
//        return elicitAttrItemList;
//    }
//
//   获取表在classTable中的id值
    public int getClassId(String fromItem) throws TMDBException {
        for(ClassTableItem item : classt.classTable) {
            if (item.classname.equals(fromItem)) {
                return item.classid;
            }
        }
        return -1;
    }
//
//    //输入需要获取的表名，得到对应的元祖值
//    public TupleList getTable(FromItem fromItem) throws TMDBException {
//        int classid=this.getClassId(((Table) fromItem).getName());
//        TupleList res=new TupleList();
//        for(ObjectTableItem item : topt.objectTable) {
//            if (item.classid == classid) {
//                Tuple tuple = this.GetTuple(item.tupleid);
////                Tuple newTuple=new Tuple();
////                newTuple.tuple=new Object[elicitAttrItemList.size()];
////                for(int i=0;i<elicitAttrItemList.size();i++){
////                    newTuple.tuple[i]=tuple.tuple[elicitAttrItemList.get(i).attrid];
////                }
//                tuple.setTupleId(item.tupleid);
//                res.addTuple(tuple);
//            }
//        }
//        return res;
//    }

    //CREATE CLASS dZ123 (nB1 int,nB2 char) ;
    //1,2,dZ123,nB1,int,nB2,char
//    public boolean CreateOriginClass(String[] p) throws TMDBException {
//        String classname = p[2];
//        int count = Integer.parseInt(p[1]);
//        classt.maxid++;
//        int classid = classt.maxid;
//        for(ClassTableItem item : classt.classTable){
//            if(item.classname.equals(classname)){
//                throw new TMDBException(classname+"已经存在！");
//            }
//        }
//        for (int i = 0; i < count; i++) {
//            classt.classTable.add(new ClassTableItem(classname, classid, count,i,p[2 * i + 3], p[2 * i + 4],"ori",null));
//        }
////        this.SaveAll();
//        return true;
//    }

    //INSERT INTO aa VALUES (1,2,"3");
    //4,3,aa,1,2,"3"
    //0 1 2  3 4  5
//    public int tupleInsert(String[] p) throws TMDBException {
//        int count = Integer.parseInt(p[1]);
//        for(int o =0;o<count+3;o++){
//            p[o] = p[o].replace("\"","");
//        }
//
//        String classname = p[2];
//        Object[] tuple_ = new Object[count];
//
//        int classid = -1;
//
//        for(ClassTableItem item:classt.classTable)
//        {
//            if(item.classname.equals(classname)){
//                if(item.attrnum!=count) throw new TMDBException("插入参数长度不匹配实际参数长度");
//                classid = item.classid;
//                break;
//            }
//        }
//        if(classid==-1) throw new TMDBException("找不到"+classname);
//
//
//
//        for(int j = 0;j<count;j++){
//            tuple_[j] = p[j+3];
//        }
//
//        Tuple tuple = new Tuple(tuple_);
//        tuple.tupleHeader=count;
//        int tupleid = topt.maxTupleId++;
//        InsertTuple(tuple);
//        topt.objectTable.add(new ObjectTableItem(classid,tupleid));
//        //向代理类加元组
//        for(DeputyTableItem item:deputyt.deputyTable){
//            if(classid == item.originid){
//                //判断代理规则
//
//                String attrtype=null;
//                int attrid=0;
//                for(ClassTableItem item1:classt.classTable){
//                    if(item1.classid == classid&&item1.attrname.equals(item.deputyrule[0])) {
//                        attrtype = item1.attrtype;
//                        attrid = item1.attrid;
//                        break;
//                    }
//                }
//
//                if(Condition(attrtype,tuple,attrid,item.deputyrule[2])){
//                    String[] ss= p.clone();
//                    String s1 = null;
//
//                    for(ClassTableItem item2:classt.classTable){
//                        if(item2.classid == item.deputyid) {
//                            s1 = item2.classname;
//                            break;
//                        }
//                    }
//                    //是否要插switch的值
//                    //收集源类属性名
//                    String[] attrname1 = new String[count];
//                    int[] attrid1 = new int[count];
//                    int k=0;
//                    for(ClassTableItem item3 : classt.classTable){
//                        if(item3.classid == classid){
//                            attrname1[k] = item3.attrname;
//                            attrid1[k] = item3.attrid;
//                            k++;
//
//                            if (k ==count)
//                                break;
//                        }
//                    }
//                    for (int l = 0;l<count;l++) {
//                        for (SwitchingTableItem item4 : switchingT.switchingTable) {
//                            if (item4.attr.equals(attrname1[l])){
//                                //判断被置换的属性是否是代理类的
//
//                                for(ClassTableItem item8: classt.classTable){
//                                    if(item8.attrname.equals(item4.deputy)&&Integer.parseInt(item4.rule)!=0){
//                                        if(item8.classid==item.deputyid){
//                                            int sw = Integer.parseInt(p[3+attrid1[l]]);
//                                            ss[3+attrid1[l]] = new Integer(sw+Integer.parseInt(item4.rule)).toString();
//                                            break;
//                                        }
//                                    }
//                                }
//
//
//                            }
//                        }
//                    }
//
//                    ss[2] = s1;
//                    int deojid=tupleInsert(ss);
//                    //插入Bi
//                    biPointerT.biPointerTable.add(new BiPointerTableItem(classid,tupleid,item.deputyid,deojid));
//                }
//            }
//        }
//        return tupleid;
//    }

    public boolean Condition(String attrtype, Tuple tuple, int attrid, String value1){
        String value = value1.replace("\"","");
        switch (attrtype){
            case "int":
                int value_int = Integer.parseInt(value);
                if(Integer.parseInt((String)tuple.tuple[attrid])==value_int)
                    return true;
                break;
            case "char":
                String value_string = value;
                if(tuple.tuple[attrid].equals(value_string))
                    return true;
                break;

        }
        return false;
    }

    public void SaveAll( ) throws IOException {
        mem.saveAll();
    }

    public void reload() throws IOException {
        mem.loadClassTable();
        mem.loadDeputyTable();
        mem.loadBiPointerTable();
        mem.loadSwitchingTable();
    }

    //CREATE SELECTDEPUTY aa SELECT  b1+2 AS c1,b2 AS c2,b3 AS c3 FROM  bb WHERE t1="1" ;
    //2,3,aa,b1,1,2,c1,b2,0,0,c2,b3,0,0,c3,bb,t1,=,"1"
    //0 1 2  3  4 5 6  7  8 9 10 11 121314 15 16 17 18
//    public boolean CreateSelectDeputy(String[] p) {
//        int count = Integer.parseInt(p[1]);
//        String classname = p[2];//代理类的名字
//        String bedeputyname = p[4*count+3];//代理的类的名字
//        classt.maxid++;
//        int classid = classt.maxid;//代理类的id
//        int bedeputyid = -1;//代理的类的id
//        String[] attrname=new String[count];
//        String[] bedeputyattrname=new String[count];
//        int[] bedeputyattrid = new int[count];
//        String[] attrtype=new String[count];
//        int[] attrid=new int[count];
//        for(int j = 0;j<count;j++){
//            attrname[j] = p[4*j+6];
//            attrid[j] = j;
//            bedeputyattrname[j] = p[4*j+3];
//        }
//
//        String attrtype1;
//        for (int i = 0; i < count; i++) {
//
//            for (ClassTableItem item:classt.classTable) {
//                if (item.classname.equals(bedeputyname)&&item.attrname.equals(p[3+4*i])) {
//                    bedeputyid = item.classid;
//                    bedeputyattrid[i] = item.attrid;
//
//                    classt.classTable.add(new ClassTableItem(classname, classid, count,attrid[i],attrname[i], item.attrtype,"de",null));
//                    //swi
//                    if(Integer.parseInt(p[4+4*i]) == 1){
//                        switchingT.switchingTable.add(new SwitchingTableItem(item.attrname,attrname[i],p[5+4*i]));
//                    }
//                    if(Integer.parseInt(p[4+4*i])==0){
//                        switchingT.switchingTable.add(new SwitchingTableItem(item.attrname,attrname[i],"0"));
//                    }
//                    break;
//                }
//            };
//        }
//
//        String[] con =new String[3];
//        con[0] = p[4+4*count];
//        con[1] = p[5+4*count];
//        con[2] = p[6+4*count];
//        deputyt.deputyTable.add(new DeputyTableItem(bedeputyid,classid,con));
//
//        TupleList tpl= new TupleList();
//
//        int conid = 0;
//        String contype  = null;
//        for(ClassTableItem item3:classt.classTable){
//            if(item3.attrname.equals(con[0])){
//                conid = item3.attrid;
//                contype = item3.attrtype;
//                break;
//            }
//        }
//        List<ObjectTableItem> obj = new ArrayList<>();
//        for(ObjectTableItem item2:topt.objectTable){
//            if(item2.classid ==bedeputyid){
//                Tuple tuple = GetTuple(item2.tupleid);
//                if(Condition(contype,tuple,conid,con[2])){
//                    //插入
//                    //swi
//                    Tuple ituple = new Tuple();
//                    ituple.tupleHeader = count;
//                    ituple.tuple = new Object[count];
//
//                    for(int o =0;o<count;o++){
//                        if(Integer.parseInt(p[4+4*o]) == 1){
//                            int value = Integer.parseInt(p[5+4*o]);
//                            int orivalue =Integer.parseInt((String)tuple.tuple[bedeputyattrid[o]]);
//                            Object ob = value+orivalue;
//                            ituple.tuple[o] = ob;
//                        }
//                        if(Integer.parseInt(p[4+4*o]) == 0){
//                            ituple.tuple[o] = tuple.tuple[bedeputyattrid[o]];
//                        }
//                    }
//
//                    topt.maxTupleId++;
//                    int tupid = topt.maxTupleId;
//
//                    InsertTuple(ituple);
//                    //topt.objectTable.add(new ObjectTableItem(classid,tupid,aa[0],aa[1]));
//                    obj.add(new ObjectTableItem(classid,tupid));
//
//                    //bi
//                    biPointerT.biPointerTable.add(new BiPointerTableItem(bedeputyid,item2.tupleid,classid,tupid));
//
//                }
//            }
//        }
//        for(ObjectTableItem item6:obj) {
//            topt.objectTable.add(item6);
//        }
//        return true;
//    }

    //UPDATE Song SET type = ‘jazz’WHERE songId = 100;
    //OPT_CREATE_UPDATE，Song，type，“jazz”，songId，=，100
    //0                  1     2      3        4      5  6
//    public ArrayList<Integer> update(String[] p){
//        String classname = p[1];
//        String attrname = p[2];
//        String cattrname = p[4];
//
//        int classid = 0;
//        int attrid = 0;
//        String attrtype = null;
//        int cattrid = 0;
//        String cattrtype = null;
//        for(ClassTableItem item :classt.classTable){
//            if (item.classname.equals(classname)){
//                classid = item.classid;
//                break;
//            }
//        }
//        for(ClassTableItem item1 :classt.classTable){
//            if (item1.classid==classid&&item1.attrname.equals(attrname)){
//                attrtype = item1.attrtype;
//                attrid = item1.attrid;
//            }
//        }
//        for(ClassTableItem item2 :classt.classTable){
//            if (item2.classid==classid&&item2.attrname.equals(cattrname)){
//                cattrtype = item2.attrtype;
//                cattrid = item2.attrid;
//            }
//        }
//
//
//        ArrayList<Integer> integers = new ArrayList<>();
//        for(ObjectTableItem item3:topt.objectTable){
//            if(item3.classid == classid){
//                Tuple tuple = GetTuple(item3.tupleid);
//                if(Condition(cattrtype,tuple,cattrid,p[6])){
//                    integers.add(item3.tupleid);
//                    UpdatebyID(item3.tupleid,attrid,p[3].replace("\"",""));
//
//                }
//            }
//        }
//        return integers;
//    }
//    private void UpdatebyID(int tupleid,int attrid,String value){
//        for(ObjectTableItem item: topt.objectTable){
//            if(item.tupleid ==tupleid){
//                Tuple tuple = GetTuple(item.tupleid);
//                tuple.tuple[attrid] = value;
//                UpateTuple(tuple,item.tupleid);
//                Tuple tuple1 = GetTuple(item.tupleid);
//                UpateTuple(tuple1,item.tupleid);
//            }
//        }
//
//        String attrname = null;
//        for(ClassTableItem item2: classt.classTable){
//            if (item2.attrid == attrid){
//                attrname = item2.attrname;
//                break;
//            }
//        }
//        for(BiPointerTableItem item1: biPointerT.biPointerTable) {
//            if (item1.objectid == tupleid) {
//
//
//                for(ClassTableItem item4:classt.classTable){
//                    if(item4.classid==item1.deputyid){
//                        String dattrname = item4.attrname;
//                        int dattrid = item4.attrid;
//                        for (SwitchingTableItem item5 : switchingT.switchingTable) {
//                            String dswitchrule = null;
//                            String dvalue = null;
//                            if (item5.attr.equals(attrname) && item5.deputy.equals(dattrname)) {
//                                dvalue = value;
//                                if (Integer.parseInt(item5.rule) != 0) {
//                                    dswitchrule = item5.rule;
//                                    dvalue = Integer.toString(Integer.parseInt(dvalue) + Integer.parseInt(dswitchrule));
//                                }
//                                UpdatebyID(item1.deputyobjectid, dattrid, dvalue);
//                                break;
//                            }
//                        }
//                    }
//                }
//            }
//        }
//
//    }

    //DELETE FROM bb WHERE t4="5SS";
    //5,bb,t4,=,"5SS"
//    public ArrayList<Integer> delete(String[] p) {
//        String classname = p[1];
//        String attrname = p[2];
//        int classid = 0;
//        int attrid=0;
//        String attrtype=null;
//        for (ClassTableItem item:classt.classTable) {
//            if (item.classname.equals(classname) && item.attrname.equals(attrname)) {
//                classid = item.classid;
//                attrid = item.attrid;
//                attrtype = item.attrtype;
//                break;
//            }
//        }
//        //寻找需要删除的
//        MemConnect.OandB ob2 = new MemConnect.OandB();
//        for (Iterator it1 = topt.objectTable.iterator(); it1.hasNext();){
//            ObjectTableItem item = (ObjectTableItem)it1.next();
//            if(item.classid == classid){
//                Tuple tuple = GetTuple(item.tupleid);
//                if(Condition(attrtype,tuple,attrid,p[4])){
//                    //需要删除的元组
//                    MemConnect.OandB ob =new MemConnect.OandB(DeletebyID(item.tupleid));
//                    for(ObjectTableItem obj:ob.o){
//                        ob2.o.add(obj);
//                    }
//                    for(BiPointerTableItem bip:ob.b){
//                        ob2.b.add(bip);
//                    }
//
//                }
//            }
//        }
//        ArrayList<Integer> integers = new ArrayList<>();
//        for(ObjectTableItem obj:ob2.o){
//            integers.add(obj.tupleid);
//            topt.objectTable.remove(obj);
//        }
//        for(BiPointerTableItem bip:ob2.b) {
//            biPointerT.biPointerTable.remove(bip);
//        }
//        return integers;
//    }
//
//    private MemConnect.OandB DeletebyID(int id){
//
//        List<ObjectTableItem> todelete1 = new ArrayList<>();
//        List<BiPointerTableItem>todelete2 = new ArrayList<>();
//        MemConnect.OandB ob = new MemConnect.OandB(todelete1,todelete2);
//        for (Iterator it1 = topt.objectTable.iterator(); it1.hasNext();){
//            ObjectTableItem item  = (ObjectTableItem)it1.next();
//            if(item.tupleid == id){
//                //需要删除的tuple
//
//
//                //删除代理类的元组
//                int deobid = 0;
//
//                for(Iterator it = biPointerT.biPointerTable.iterator(); it.hasNext();){
//                    BiPointerTableItem item1 =(BiPointerTableItem) it.next();
//                    if(item.tupleid == item1.deputyobjectid){
//                        //it.remove();
//                        if(!todelete2.contains(item1))
//                            todelete2.add(item1);
//                    }
//                    if(item.tupleid == item1.objectid){
//                        deobid = item1.deputyobjectid;
//                        MemConnect.OandB ob2=new MemConnect.OandB(DeletebyID(deobid));
//
//                        for(ObjectTableItem obj:ob2.o){
//                            if(!todelete1.contains(obj))
//                                todelete1.add(obj);
//                        }
//                        for(BiPointerTableItem bip:ob2.b){
//                            if(!todelete2.contains(bip))
//                                todelete2.add(bip);
//                        }
//
//                        //biPointerT.biPointerTable.remove(item1);
//
//                    }
//                }
//
//
//                //删除自身
//                DeleteTuple(item.tupleid);
//                if(!todelete2.contains(item));
//                todelete1.add(item);
//
//            }
//        }
//
//        return ob;
//    }

    public static class OandB{
        public List<ObjectTableItem> o= new ArrayList<>();
        public List<BiPointerTableItem> b= new ArrayList<>();
        public OandB(){}
        public OandB(MemConnect.OandB oandB){
            this.o = oandB.o;
            this.b = oandB.b;
        }

        public OandB(List<ObjectTableItem> o, List<BiPointerTableItem> b) {
            this.o = o;
            this.b = b;
        }
    }

//    //DROP CLASS asd;
//    //3,asd
//    public boolean drop(String[]p) throws TMDBException {
//        List<DeputyTableItem> dti;
//        dti = Drop1(p);
//        for(DeputyTableItem item:dti){
//            deputyt.deputyTable.remove(item);
//        }
//        return  true;
//    }
//
//    private List<DeputyTableItem> Drop1(String[] p) throws TMDBException {
//        String classname = p[1];
//        int classid = getClassId(p[1]);
//        //找到classid顺便 清除类表和switch表
//
//        for (Iterator it1 = classt.classTable.iterator(); it1.hasNext();) {
//            ClassTableItem item =(ClassTableItem) it1.next();
//            if (item.classname.equals(classname) ){
//                classid = item.classid;
//                for(Iterator it = switchingT.switchingTable.iterator(); it.hasNext();) {
//                    SwitchingTableItem item2 =(SwitchingTableItem) it.next();
//                    if (item2.attr.equals( item.attrname)||item2.deputy .equals( item.attrname)){
//                        it.remove();
//                    }
//                }
//                it1.remove();
//            }
//        }
//        //清元组表同时清了bi
//        MemConnect.OandB ob2 = new MemConnect.OandB();
//        for(ObjectTableItem item1:topt.objectTable){
//            if(item1.classid == classid){
//                MemConnect.OandB ob = DeletebyID(item1.tupleid);
//                for(ObjectTableItem obj:ob.o){
//                    ob2.o.add(obj);
//                }
//                for(BiPointerTableItem bip:ob.b){
//                    ob2.b.add(bip);
//                }
//            }
//        }
//        for(ObjectTableItem obj:ob2.o){
//            topt.objectTable.remove(obj);
//        }
//        for(BiPointerTableItem bip:ob2.b) {
//            biPointerT.biPointerTable.remove(bip);
//        }
//
//        //清deputy
//        List<DeputyTableItem> dti = new ArrayList<>();
//        for(DeputyTableItem item3:deputyt.deputyTable){
//            if(item3.deputyid == classid){
//                if(!dti.contains(item3))
//                    dti.add(item3);
//            }
//            if(item3.originid == classid){
//                //删除代理类
//                String[]s = p.clone();
//                List<String> sname = new ArrayList<>();
//                for(ClassTableItem item5: classt.classTable) {
//                    if (item5.classid == item3.deputyid) {
//                        sname.add(item5.classname);
//                    }
//                }
//                for(String item4: sname){
//
//                    s[1] = item4;
//                    List<DeputyTableItem> dti2 = Drop1(s);
//                    for(DeputyTableItem item8:dti2){
//                        if(!dti.contains(item8))
//                            dti.add(item8);
//                    }
//
//                }
//                if(!dti.contains(item3))
//                    dti.add(item3);
//            }
//        }
//        return dti;
//
//    }

    public static ObjectTable getTopt() {
        return topt;
    }

    public static ClassTable getClasst() {
        return classt;
    }

    public static DeputyTable getDeputyt() {
        return deputyt;
    }

    public static BiPointerTable getBiPointerT() {
        return biPointerT;
    }

    public static SwitchingTable getSwitchingT() {
        return switchingT;
    }
}
