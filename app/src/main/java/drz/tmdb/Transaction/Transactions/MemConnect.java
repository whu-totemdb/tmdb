package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

import drz.tmdb.Level.LevelManager;
import drz.tmdb.Log.LogManager;
import drz.tmdb.Memory.MemManage;
import drz.tmdb.Memory.MemManager;
import drz.tmdb.Memory.Tuple;
import drz.tmdb.Memory.TupleList;
import drz.tmdb.Transaction.SystemTable.BiPointerTable;
import drz.tmdb.Transaction.SystemTable.BiPointerTableItem;
import drz.tmdb.Transaction.SystemTable.ClassTable;
import drz.tmdb.Transaction.SystemTable.ClassTableItem;
import drz.tmdb.Transaction.SystemTable.DeputyTable;
import drz.tmdb.Transaction.SystemTable.DeputyTableItem;
import drz.tmdb.Transaction.SystemTable.ObjectTable;
import drz.tmdb.Transaction.SystemTable.ObjectTableItem;
import drz.tmdb.Transaction.SystemTable.SwitchingTable;
import drz.tmdb.Transaction.SystemTable.SwitchingTableItem;
import drz.tmdb.Transaction.TransAction;

public class MemConnect {
    private static MemManage mem = new MemManage();
    private static ObjectTable topt = mem.loadObjectTable();
    private static ClassTable classt = mem.loadClassTable();
    private static DeputyTable deputyt = mem.loadDeputyTable();
    private static BiPointerTable biPointerT = mem.loadBiPointerTable();
    private static SwitchingTable switchingT = mem.loadSwitchingTable();

//        LogManager log = new LogManager(this);

    public MemConnect(){
    };

    public Tuple GetTuple(int id, int offset) {
        return mem.readTuple(id,offset);
    }

    public int[] InsertTuple(Tuple tuple){
        return mem.writeTuple(tuple);
    }

    public void DeleteTuple(int id, int offset){
        mem.deleteTuple();
        return;
    }

    public void UpateTuple(Tuple tuple,int blockid,int offset){
        mem.UpateTuple(tuple,blockid,offset);
    }

    public ArrayList<ClassTableItem> getSelectItem(FromItem fromItem){
        ArrayList<ClassTableItem> elicitAttrItemList=new ArrayList<>();
        for(ClassTableItem item : classt.classTable){
            if(item.classname.equals(((Table)fromItem).getName())){
                ClassTableItem temp=item.getCopy();
                if(fromItem.getAlias()!=null) temp.classname=fromItem.getAlias().getName();
                elicitAttrItemList.add(temp);
            }
        }
        return elicitAttrItemList;
    }

    public ArrayList<ClassTableItem> getSelectItem(FromItem fromItem, List<Column> columnList){
        // 从class表中提取将要获取的元素。
        ArrayList<ClassTableItem> elicitAttrItemList=new ArrayList<>();
        for(ClassTableItem item : classt.classTable){
            if(item.classname.equals(fromItem.toString())){
                String attrName=item.attrname;
                boolean flag=false;
                for(Column column:columnList){
                    Column c=(Column) column;
                    if(c.getTable()!=null && !(c.getTable().equals(fromItem.toString())&& c.getTable().equals(fromItem.getAlias().getName()))) continue;
                    if(attrName.equals(c.getColumnName())) {
                        flag=true;
                        break;
                    }
                }
                if(flag) elicitAttrItemList.add(item);
            }
        }
        return elicitAttrItemList;
    }

    public int getClassId(String fromItem){
        for(ClassTableItem item : classt.classTable) {
            if (item.classname.equals(fromItem)) {
                return item.classid;
            }
        }
        return -1;
    }

    public TupleList getTable(FromItem fromItem){
        int classid=this.getClassId(((Table) fromItem).getName());
        TupleList res=new TupleList();
        for(ObjectTableItem item : topt.objectTable) {
            if (item.classid == classid) {
                Tuple tuple = this.GetTuple(item.blockid,item.offset);
//                Tuple newTuple=new Tuple();
//                newTuple.tuple=new Object[elicitAttrItemList.size()];
//                for(int i=0;i<elicitAttrItemList.size();i++){
//                    newTuple.tuple[i]=tuple.tuple[elicitAttrItemList.get(i).attrid];
//                }
                res.addTuple(tuple);
            }
        }
        return res;
    }

    //CREATE CLASS dZ123 (nB1 int,nB2 char) ;
    //1,2,dZ123,nB1,int,nB2,char
    public boolean CreateOriginClass(String[] p) {
        String classname = p[2];
        int count = Integer.parseInt(p[1]);
        classt.maxid++;
        int classid = classt.maxid;
        for(ClassTableItem item : classt.classTable){
            if(item.classname.equals(classname)){
                return false;
            }
        }
        for (int i = 0; i < count; i++) {
            classt.classTable.add(new ClassTableItem(classname, classid, count,i,p[2 * i + 3], p[2 * i + 4],"ori"));
        }
//        this.SaveAll();
        return true;
    }

    //INSERT INTO aa VALUES (1,2,"3");
    //4,3,aa,1,2,"3"
    //0 1 2  3 4  5
    public int tupleInsert(String[] p){
        int count = Integer.parseInt(p[1]);
        for(int o =0;o<count+3;o++){
            p[o] = p[o].replace("\"","");
        }

        String classname = p[2];
        Object[] tuple_ = new Object[count];

        int classid = 0;

        for(ClassTableItem item:classt.classTable)
        {
            if(item.classname.equals(classname)){
                classid = item.classid;
                break;
            }
        }
        if(classid==0) return -1;


        for(int j = 0;j<count;j++){
            tuple_[j] = p[j+3];
        }

        Tuple tuple = new Tuple(tuple_);
        tuple.tupleHeader=count;

        int[] a = InsertTuple(tuple);
        topt.maxTupleId++;
        int tupleid = topt.maxTupleId;
        topt.objectTable.add(new ObjectTableItem(classid,tupleid,a[0],a[1]));
        //向代理类加元组
        for(DeputyTableItem item:deputyt.deputyTable){
            if(classid == item.originid){
                //判断代理规则

                String attrtype=null;
                int attrid=0;
                for(ClassTableItem item1:classt.classTable){
                    if(item1.classid == classid&&item1.attrname.equals(item.deputyrule[0])) {
                        attrtype = item1.attrtype;
                        attrid = item1.attrid;
                        break;
                    }
                }

                if(Condition(attrtype,tuple,attrid,item.deputyrule[2])){
                    String[] ss= p.clone();
                    String s1 = null;

                    for(ClassTableItem item2:classt.classTable){
                        if(item2.classid == item.deputyid) {
                            s1 = item2.classname;
                            break;
                        }
                    }
                    //是否要插switch的值
                    //收集源类属性名
                    String[] attrname1 = new String[count];
                    int[] attrid1 = new int[count];
                    int k=0;
                    for(ClassTableItem item3 : classt.classTable){
                        if(item3.classid == classid){
                            attrname1[k] = item3.attrname;
                            attrid1[k] = item3.attrid;
                            k++;

                            if (k ==count)
                                break;
                        }
                    }
                    for (int l = 0;l<count;l++) {
                        for (SwitchingTableItem item4 : switchingT.switchingTable) {
                            if (item4.attr.equals(attrname1[l])){
                                //判断被置换的属性是否是代理类的

                                for(ClassTableItem item8: classt.classTable){
                                    if(item8.attrname.equals(item4.deputy)&&Integer.parseInt(item4.rule)!=0){
                                        if(item8.classid==item.deputyid){
                                            int sw = Integer.parseInt(p[3+attrid1[l]]);
                                            ss[3+attrid1[l]] = new Integer(sw+Integer.parseInt(item4.rule)).toString();
                                            break;
                                        }
                                    }
                                }


                            }
                        }
                    }

                    ss[2] = s1;
                    int deojid=tupleInsert(ss);
                    //插入Bi
                    biPointerT.biPointerTable.add(new BiPointerTableItem(classid,tupleid,item.deputyid,deojid));
                }
            }
        }
        return tupleid;
    }

    private boolean Condition(String attrtype,Tuple tuple,int attrid,String value1){
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

    public void SaveAll( )
    {

        mem.saveObjectTable(topt);
        mem.saveClassTable(classt);
        mem.saveDeputyTable(deputyt);
        mem.saveBiPointerTable(biPointerT);
        mem.saveSwitchingTable(switchingT);
//        mem.saveLog(log.LogT);
//        while(!mem.flush());
//        while(!mem.setLogCheck(log.LogT.logID));
//        mem.setCheckPoint(log.LogT.logID);//成功退出,所以新的事务块一定全部执行
        MemManager memManager = new MemManager(topt.objectTable, classt.classTable,
                deputyt.deputyTable, biPointerT.biPointerTable, switchingT.switchingTable);
        LevelManager levelManager = memManager.levelManager;
        memManager.saveMemTableToFile();// 先保存memTable再保存index，因为memTable保存的过程中可能会修改index
        levelManager.saveIndexToFile();
    }

    public void reload(){
        this.topt = mem.loadObjectTable();
        this.classt = mem.loadClassTable();
        this.deputyt = mem.loadDeputyTable();
        this.biPointerT = mem.loadBiPointerTable();
        this.switchingT = mem.loadSwitchingTable();
    }

    //CREATE SELECTDEPUTY aa SELECT  b1+2 AS c1,b2 AS c2,b3 AS c3 FROM  bb WHERE t1="1" ;
    //2,3,aa,b1,1,2,c1,b2,0,0,c2,b3,0,0,c3,bb,t1,=,"1"
    //0 1 2  3  4 5 6  7  8 9 10 11 121314 15 16 17 18
    private void CreateSelectDeputy(String[] p) {
        int count = Integer.parseInt(p[1]);
        String classname = p[2];//代理类的名字
        String bedeputyname = p[4*count+3];//代理的类的名字
        classt.maxid++;
        int classid = classt.maxid;//代理类的id
        int bedeputyid = -1;//代理的类的id
        String[] attrname=new String[count];
        String[] bedeputyattrname=new String[count];
        int[] bedeputyattrid = new int[count];
        String[] attrtype=new String[count];
        int[] attrid=new int[count];
        for(int j = 0;j<count;j++){
            attrname[j] = p[4*j+6];
            attrid[j] = j;
            bedeputyattrname[j] = p[4*j+3];
        }

        String attrtype1;
        for (int i = 0; i < count; i++) {

            for (ClassTableItem item:classt.classTable) {
                if (item.classname.equals(bedeputyname)&&item.attrname.equals(p[3+4*i])) {
                    bedeputyid = item.classid;
                    bedeputyattrid[i] = item.attrid;

                    classt.classTable.add(new ClassTableItem(classname, classid, count,attrid[i],attrname[i], item.attrtype,"de"));
                    //swi
                    if(Integer.parseInt(p[4+4*i]) == 1){
                        switchingT.switchingTable.add(new SwitchingTableItem(item.attrname,attrname[i],p[5+4*i]));
                    }
                    if(Integer.parseInt(p[4+4*i])==0){
                        switchingT.switchingTable.add(new SwitchingTableItem(item.attrname,attrname[i],"0"));
                    }
                    break;
                }
            };
        }



        String[] con =new String[3];
        con[0] = p[4+4*count];
        con[1] = p[5+4*count];
        con[2] = p[6+4*count];
        deputyt.deputyTable.add(new DeputyTableItem(bedeputyid,classid,con));


        TupleList tpl= new TupleList();

        int conid = 0;
        String contype  = null;
        for(ClassTableItem item3:classt.classTable){
            if(item3.attrname.equals(con[0])){
                conid = item3.attrid;
                contype = item3.attrtype;
                break;
            }
        }
        List<ObjectTableItem> obj = new ArrayList<>();
        for(ObjectTableItem item2:topt.objectTable){
            if(item2.classid ==bedeputyid){
                Tuple tuple = GetTuple(item2.blockid,item2.offset);
                if(Condition(contype,tuple,conid,con[2])){
                    //插入
                    //swi
                    Tuple ituple = new Tuple();
                    ituple.tupleHeader = count;
                    ituple.tuple = new Object[count];

                    for(int o =0;o<count;o++){
                        if(Integer.parseInt(p[4+4*o]) == 1){
                            int value = Integer.parseInt(p[5+4*o]);
                            int orivalue =Integer.parseInt((String)tuple.tuple[bedeputyattrid[o]]);
                            Object ob = value+orivalue;
                            ituple.tuple[o] = ob;
                        }
                        if(Integer.parseInt(p[4+4*o]) == 0){
                            ituple.tuple[o] = tuple.tuple[bedeputyattrid[o]];
                        }
                    }

                    topt.maxTupleId++;
                    int tupid = topt.maxTupleId;

                    int [] aa = InsertTuple(ituple);
                    //topt.objectTable.add(new ObjectTableItem(classid,tupid,aa[0],aa[1]));
                    obj.add(new ObjectTableItem(classid,tupid,aa[0],aa[1]));

                    //bi
                    biPointerT.biPointerTable.add(new BiPointerTableItem(bedeputyid,item2.tupleid,classid,tupid));

                }
            }
        }
        for(ObjectTableItem item6:obj) {
            topt.objectTable.add(item6);
        }
    }

    public void createDeputyClass(CreateDeputyClass stmt){
        Table deputyClass=stmt.getDeputyClass();

    }

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
