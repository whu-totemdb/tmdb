package drz.tmdb.Transaction;

import static drz.tmdb.level.Test.*;

import android.app.AlertDialog;
import android.content.Context;
import android.content.Intent;
import android.os.Bundle;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

//import drz.tmdb.Level.LevelManager;
import drz.tmdb.level.LevelManager;
import drz.tmdb.Log.*;
import drz.tmdb.memory.*;


import drz.tmdb.memory.SystemTable.*;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.Create;
import drz.tmdb.Transaction.Transactions.CreateDeputyClass;
import drz.tmdb.Transaction.Transactions.Delete;
import drz.tmdb.Transaction.Transactions.Drop;
import drz.tmdb.Transaction.Transactions.Insert;
import drz.tmdb.Transaction.Transactions.Update;
import drz.tmdb.Transaction.Transactions.impl.CreateImpl;
import drz.tmdb.Transaction.Transactions.impl.CreateDeputyClassImpl;
import drz.tmdb.Transaction.Transactions.impl.DeleteImpl;
import drz.tmdb.Transaction.Transactions.impl.DropImpl;
import drz.tmdb.Transaction.Transactions.impl.InsertImpl;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;
import drz.tmdb.Transaction.Transactions.Select;
import drz.tmdb.Transaction.Transactions.impl.SelectImpl;
import drz.tmdb.Transaction.Transactions.utils.SelectResult;
import drz.tmdb.Transaction.Transactions.impl.UpdateImpl;
import drz.tmdb.map.TrajectoryUtils;
import drz.tmdb.show.PrintResult;
import drz.tmdb.show.ShowTable;

public class TransAction {

    Context context;
    public MemManager mem;
    public LevelManager levelManager;
    public LogManager log;

    private MemConnect memConnect;

    public TransAction() throws IOException {}

    public TransAction(Context context) throws IOException {
        //test23();
        this.context = context;
        this.mem = new MemManager();
        this.levelManager = mem.levelManager;
        this.memConnect=new MemConnect(mem);
        new TrajectoryUtils(memConnect);

//        topt = mem.objectTable;
//        classt = mem.classTable;
//        deputyt = mem.deputyTable;
//        biPointerT = mem.biPointerTable;
//        switchingT = mem.switchingTable;
    }


    public void clear() throws IOException {
//        File classtab=new File("/data/data/drz.tmdb/transaction/classtable");
//        classtab.delete();
        File objtab=new File("/data/data/drz.tmdb/transaction/objecttable");
        objtab.delete();
    }

    public void SaveAll( ) throws IOException {
        memConnect.SaveAll();
    }

    public void reload() throws IOException {
        memConnect.reload();
    }

    public void Test(){
        TupleList tpl = new TupleList();
        Tuple t1 = new Tuple();
        t1.tupleHeader = 5;
        t1.tuple = new Object[t1.tupleHeader];
        t1.tuple[0] = "a";
        t1.tuple[1] = 1;
        t1.tuple[2] = "b";
        t1.tuple[3] = 3;
        t1.tuple[4] = "e";
        Tuple t2 = new Tuple();
        t2.tupleHeader = 5;
        t2.tuple = new Object[t2.tupleHeader];
        t2.tuple[0] = "d";
        t2.tuple[1] = 2;
        t2.tuple[2] = "e";
        t2.tuple[3] = 2;
        t2.tuple[4] = "v";

        tpl.addTuple(t1);
        tpl.addTuple(t2);
        String[] attrname = {"attr2","attr1","attr3","attr5","attr4"};
        int[] attrid = {1,0,2,4,3};
        String[]attrtype = {"int","char","char","char","int"};

        PrintSelectResult(tpl,attrname,attrid,attrtype);

//        int[] a = InsertTuple(t1);
//        Tuple t3 = GetTuple(a[0],a[1]);
//        int[] b = InsertTuple(t2);
//        Tuple t4 = GetTuple(b[0],b[1]);
//        System.out.println(t3);
    }
/**
    private boolean RedoRest(){//redo
        LogTable redo;
        if((redo=log.GetReDo())!=null) {
            int redonum = redo.logTable.size();   //先把redo指令加前面
            for (int i = 0; i < redonum; i++) {
                int id=redo.logTable.get(i).logid;
                int op = redo.logTable.get(i).op;
                String k = redo.logTable.get(i).key;
                String s = redo.logTable.get(i).value;

                log.WriteLog(id,k,op,s);
                query2(id,k,op,s);
//                query2(s);
            }
        }else{
            return false;
        }
        return true;
    }
 **/

//    public String query(String s) {
//        this.reload();
//        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
//        parse p = new parse(byteArrayInputStream);
//        try {
//            String[] aa = p.Run();
//
//            switch (Integer.parseInt(aa[0])) {
//                case parse.OPT_CREATE_ORIGINCLASS:
//                    log.WriteLog(s);
//                    if(CreateOriginClass(aa)) new AlertDialog.Builder(context).setTitle("提示").setMessage("创建成功").setPositiveButton("确定",null).show();
//                    else new AlertDialog.Builder(context).setTitle("提示").setMessage("创建失败").setPositiveButton("确定",null).show();
//                    break;
//                case parse.OPT_CREATE_SELECTDEPUTY:
//                    log.WriteLog(s);
//                    CreateSelectDeputy(aa);
//                    new AlertDialog.Builder(context).setTitle("提示").setMessage("创建成功").setPositiveButton("确定",null).show();
//                    break;
//                case parse.OPT_DROP:
//                    log.WriteLog(s);
//                    Drop(aa);
//                    new AlertDialog.Builder(context).setTitle("提示").setMessage("删除成功").setPositiveButton("确定",null).show();
//                    break;
//                case parse.OPT_INSERT:
//                    log.WriteLog(s);
//                    Insert(aa);
//                    new AlertDialog.Builder(context).setTitle("提示").setMessage("插入成功").setPositiveButton("确定",null).show();
//                    break;
//                case parse.OPT_DELETE:
//                    log.WriteLog(s);
//                    Delete(aa);
//                    new AlertDialog.Builder(context).setTitle("提示").setMessage("删除成功").setPositiveButton("确定",null).show();
//                    break;
//                case parse.OPT_SELECT_DERECTSELECT:
//                    DirectSelect(aa);
//                    break;
//                case parse.OPT_SELECT_INDERECTSELECT:
//                    InDirectSelect(aa);
//                    break;
//                case parse.OPT_CREATE_UPDATE:
//                    log.WriteLog(s);
//                    Update(aa);
//                    new AlertDialog.Builder(context).setTitle("提示").setMessage("更新成功").setPositiveButton("确定",null).show();
//                default:
//                    break;
//
//            }
//        } catch (ParseException e) {ååå
//
//            e.printStackTrace();
//        }
//
//        return s;
//    }

    public String query2(String k, int op, String s) {
//        memConnect.reload();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
        //Action action = new Action();
//        action.generate(s);
        ArrayList<Integer> tuples=new ArrayList<>();
        try {
            //使用JSqlparser进行sql语句解析，会根据sql类型生成对应的语法树。
            Statement stmt= CCJSqlParserUtil.parse(byteArrayInputStream);
//            String[] aa = new String[2];
            //获取生成语法树的类型，用于进一步判断
            String sqlType=stmt.getClass().getSimpleName();

            switch (sqlType) {
                case "CreateTable":
//                    log.WrteLog(s);
                    Create create =new CreateImpl(memConnect);
                    if(create.create(stmt)) new AlertDialog.Builder(context).setTitle("提示").setMessage("创建成功").setPositiveButton("确定",null).show();
                    else new AlertDialog.Builder(context).setTitle("提示").setMessage("创建失败").setPositiveButton("确定",null).show();
                    break;
                case "CreateDeputyClass":
//                    switch
 //                   log.WriteLog(id,k,op,s);
                    CreateDeputyClass createDeputyClass=new CreateDeputyClassImpl(memConnect);
                    if(createDeputyClass.createDeputyClass(stmt)) {
                        new AlertDialog.Builder(context).setTitle("提示").setMessage("创建成功").setPositiveButton("确定",null).show();
                    }
                    break;
//                case "Create":
//                    log.WriteLog(s);
//                    CreateUnionDeputy(aa);
//                    new AlertDialog.Builder(context).setTitle("提示").setMessage("创建Union代理类成功").setPositiveButton("确定",null).show();
//                    break;
                case "Drop":
//                    log.WriteLog(id,k,op,s);
                    Drop drop=new DropImpl(memConnect);
                    drop.drop(stmt);
                    new AlertDialog.Builder(context).setTitle("提示").setMessage("删除成功").setPositiveButton("确定",null).show();
                    break;
                case "Insert":
//                    log.WriteLog(id,k,op,s);
                    Insert insert=new InsertImpl(memConnect);
                    tuples=insert.insert(stmt);
                    new AlertDialog.Builder(context).setTitle("提示").setMessage("插入成功").setPositiveButton("确定",null).show();
                    break;
                case "Delete":
 //                   log.WriteLog(id,k,op,s);
                    Delete delete=new DeleteImpl(memConnect);
                    tuples= delete.delete(stmt);
                    new AlertDialog.Builder(context).setTitle("提示").setMessage("删除成功").setPositiveButton("确定",null).show();
                    break;
                case "Select":
                    Select select=new SelectImpl(memConnect);
                    SelectResult selectResult=select.select((net.sf.jsqlparser.statement.select.Select) stmt);
                    for (Tuple t:
                         selectResult.getTpl().tuplelist) {
                        tuples.add(t.getTupleId());
                    }

                    this.PrintSelectResult(selectResult);
                    break;
//                case parse.OPT_SELECT_INDERECTSELECT:
//                    InDirectSelect(aa);
//                    break;
                case "Update":
 //                   log.WriteLog(id,k,op,s);
                    Update update=new UpdateImpl(memConnect);
                    tuples=update.update(stmt);
                    new AlertDialog.Builder(context).setTitle("提示").setMessage("更新成功").setPositiveButton("确定",null).show();
                    break;
//                case :
//                    log.WriteLog(s);
//                    Union(aa);
//                    //new AlertDialog.Builder(context).setTitle("提示").setMessage("合并成功").setPositiveButton("确定",null).show();
//                    break;
                default:
                    break;

            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
            new AlertDialog.Builder(context).setTitle("提示").setMessage("SQL语法错误").setPositiveButton("确定",null).show();
        } catch (TMDBException e) {
            e.printStackTrace();
            new AlertDialog.Builder(context).setTitle("提示").setMessage(e.getMessage()).setPositiveButton("确定",null).show();
        }
        int[] ints = new int[tuples.size()];
        for (int i = 0; i < tuples.size(); i++) {
            ints[i]=tuples.get(i);
        }
//        action.setKey(ints);
        return s;
    }

    //CREATE CLASS dZ123 (nB1 int,nB2 char) ;
    //1,2,dZ123,nB1,int,nB2,char
//    private boolean CreateOriginClass(String[] p) {
//        String classname = p[2];
//        int count = Integer.parseInt(p[1]);
//        classt.maxid++;
//        int classid = classt.maxid;
//        for(ClassTableItem item : classt.classTable){
//            if(item.classname.equals(classname)){
//                return false;
//            }
//        }
//        for (int i = 0; i < count; i++) {
//            classt.classTable.add(new ClassTableItem(classname, classid, count,i,p[2 * i + 3], p[2 * i + 4],"ori",""));
//        }
////        this.SaveAll();
//        mem.loadClassTable();
//        return true;
//    }
//
//    //INSERT INTO aa VALUES (1,2,"3");
//    //4,3,aa,1,2,"3"
//    //0 1 2  3 4  5
//    private int Insert(String[] p){
//
//
//        int count = Integer.parseInt(p[1]);
//        for(int o =0;o<count+3;o++){
//            p[o] = p[o].replace("\"","");
//        }
//
//        String classname = p[2];
//        Object[] tuple_ = new Object[count];
//
//        int classid = 0;
//
//        for(ClassTableItem item:classt.classTable)
//        {
//            if(item.classname.equals(classname)){
//                classid = item.classid;
//                break;
//            }
//        }
//
//        for(int j = 0;j<count;j++){
//            tuple_[j] = p[j+3];
//        }
//
//        Tuple tuple = new Tuple(tuple_);
//        tuple.tupleHeader=count;
//
//        int[] a = InsertTuple(tuple);
//        topt.maxTupleId++;
//        int tupleid = topt.maxTupleId;
//        topt.objectTable.add(new ObjectTableItem(classid,tupleid,a[0],a[1]));
//
//        //向代理类加元组
//
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
//                    int deojid=Insert(ss);
//                    //插入Bi
//                    biPointerT.biPointerTable.add(new BiPointerTableItem(classid,tupleid,item.deputyid,deojid));
//                }
//            }
//        }
//        return tupleid;
//    }
//
//    private boolean Condition(String attrtype,Tuple tuple,int attrid,String value1){
//        String value = value1.replace("\"","");
//        switch (attrtype){
//            case "int":
//                int value_int = Integer.parseInt(value);
//                if(Integer.parseInt((String)tuple.tuple[attrid])==value_int)
//                    return true;
//                break;
//            case "char":
//                String value_string = value;
//                if(tuple.tuple[attrid].equals(value_string))
//                    return true;
//                break;
//
//        }
//        return false;
//    }
//    //DELETE FROM bb WHERE t4="5SS";
//    //5,bb,t4,=,"5SS"
//    private void Delete(String[] p) {
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
//        OandB ob2 = new OandB();
//        for (Iterator it1 = topt.objectTable.iterator(); it1.hasNext();){
//            ObjectTableItem item = (ObjectTableItem)it1.next();
//            if(item.classid == classid){
//                Tuple tuple = GetTuple(item.blockid,item.offset);
//                if(Condition(attrtype,tuple,attrid,p[4])){
//                    //需要删除的元组
//                    OandB ob =new OandB(DeletebyID(item.tupleid));
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
//        for(ObjectTableItem obj:ob2.o){
//            topt.objectTable.remove(obj);
//        }
//        for(BiPointerTableItem bip:ob2.b) {
//            biPointerT.biPointerTable.remove(bip);
//        }
//
//    }
//
//    private OandB DeletebyID(int id){
//
//        List<ObjectTableItem> todelete1 = new ArrayList<>();
//        List<BiPointerTableItem>todelete2 = new ArrayList<>();
//        OandB ob = new OandB(todelete1,todelete2);
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
//                        OandB ob2=new OandB(DeletebyID(deobid));
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
//                DeleteTuple(item.blockid,item.offset);
//                if(!todelete2.contains(item));
//                todelete1.add(item);
//
//
//
//
//
//            }
//        }
//
//        return ob;
//    }
//
//    //DROP CLASS asd;
//    //3,asd
//
//    private void Drop(String[]p){
//        List<DeputyTableItem> dti;
//        dti = Drop1(p);
//        for(DeputyTableItem item:dti){
//            deputyt.deputyTable.remove(item);
//        }
//    }
//
//    private List<DeputyTableItem> Drop1(String[] p){
//        String classname = p[1];
//        int classid = 0;
//        //找到classid顺便 清除类表和switch表
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
//        OandB ob2 = new OandB();
//        for(ObjectTableItem item1:topt.objectTable){
//            if(item1.classid == classid){
//                OandB ob = DeletebyID(item1.tupleid);
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
//
//
//    //SELECT  b1+2 AS c1,b2 AS c2,b3 AS c3 FROM  bb WHERE t1="1";
//    //6,3,b1,1,2,c1,b2,0,0,c2,b3,0,0,c3,bb,t1,=,"1"
//    //0 1 2  3 4 5  6  7 8 9  10 111213 14 15 16 17
//    private TupleList DirectSelect(String[] p){
//        TupleList tpl = new TupleList();
//        int attrnumber = Integer.parseInt(p[1]);
//        String[] attrname = new String[attrnumber];
//        int[] attrid = new int[attrnumber];
//        String[] attrtype= new String[attrnumber];
//        String classname = p[2+4*attrnumber];
//        int classid = 0;
//        for(int i = 0;i < attrnumber;i++){
//            for (ClassTableItem item:classt.classTable) {
//                if (item.classname.equals(classname) && item.attrname.equals(p[2+4*i])) {
//                    classid = item.classid;
//                    attrid[i] = item.attrid;
//                    attrtype[i] = item.attrtype;
//                    attrname[i] = p[5+4*i];
//                    //重命名
//                    break;
//                }
//            }
//        }
//
//
//        int sattrid = 0;
//        String sattrtype = null;
//        for (ClassTableItem item:classt.classTable) {
//            if (item.classid == classid && item.attrname.equals(p[3+4*attrnumber])) {
//                sattrid = item.attrid;
//                sattrtype = item.attrtype;
//                break;
//            }
//        }
//
//
//        for(ObjectTableItem item : topt.objectTable){
//            if(item.classid == classid){
//                Tuple tuple = GetTuple(item.blockid,item.offset);
//                if(Condition(sattrtype,tuple,sattrid,p[4*attrnumber+5])){
//                    //Switch
//                    for(int j = 0;j<attrnumber;j++){
//                        if(Integer.parseInt(p[3+4*j])==1){
//                            int value = Integer.parseInt(p[4+4*j]);
//                            int orivalue = Integer.parseInt((String)tuple.tuple[attrid[j]]);
//                            Object ob = value+orivalue;
//                            tuple.tuple[attrid[j]] = ob;
//                        }
//                    }
//                    tpl.addTuple(tuple);
//                }
//            }
//        }
//        for(int i =0;i<attrnumber;i++){
//            attrid[i]=i;
//        }
////        PrintSelectResult(new SelectResult(tpl,attrname,attrid,attrtype));
//        return tpl;
//
//    }
//
//    //CREATE SELECTDEPUTY aa SELECT  b1+2 AS c1,b2 AS c2,b3 AS c3 FROM  bb WHERE t1="1" ;
//    //2,3,aa,b1,1,2,c1,b2,0,0,c2,b3,0,0,c3,bb,t1,=,"1"
//    //0 1 2  3  4 5 6  7  8 9 10 11 121314 15 16 17 18
//    private void CreateSelectDeputy(String[] p) {
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
//                    classt.classTable.add(new ClassTableItem(classname, classid, count,attrid[i],attrname[i], item.attrtype,"de",""));
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
//
//
//        String[] con =new String[3];
//        con[0] = p[4+4*count];
//        con[1] = p[5+4*count];
//        con[2] = p[6+4*count];
//        deputyt.deputyTable.add(new DeputyTableItem(bedeputyid,classid,con));
//
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
//                Tuple tuple = GetTuple(item2.blockid,item2.offset);
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
//                    int [] aa = InsertTuple(ituple);
//                    //topt.objectTable.add(new ObjectTableItem(classid,tupid,aa[0],aa[1]));
//                    obj.add(new ObjectTableItem(classid,tupid,aa[0],aa[1]));
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
//    }
//
//    //SELECT popSinger -> singer.nation  FROM popSinger WHERE singerName = "JayZhou";
//    //7,2,popSinger,singer,nation,popSinger,singerName,=,"JayZhou"
//    //0 1 2         3      4      5         6          7  8
//    private TupleList InDirectSelect(String[] p){
//        TupleList tpl= new TupleList();
//        String classname = p[3];
//        String attrname = p[4];
//        String crossname = p[2];
//        String[] attrtype = new String[1];
//        String[] con =new String[3];
//        con[0] = p[6];
//        con[1] = p[7];
//        con[2] = p[8];
//
//        int classid = 0;
//        int crossid = 0;
//        String crossattrtype = null;
//        int crossattrid = 0;
//        for(ClassTableItem item : classt.classTable){
//            if(item.classname.equals(classname)){
//                classid = item.classid;
//                if(attrname.equals(item.attrname))
//                    attrtype[0]=item.attrtype;
//            }
//            if(item.classname.equals(crossname)){
//                crossid = item.classid;
//                if(item.attrname.equals(con[0])) {
//                    crossattrtype = item.attrtype;
//                    crossattrid = item.attrid;
//                }
//            }
//        }
//
//        for(ObjectTableItem item1:topt.objectTable){
//            if(item1.classid == crossid){
//                Tuple tuple = GetTuple(item1.blockid,item1.offset);
//                if(Condition(crossattrtype,tuple,crossattrid,con[2])){
//                    for(BiPointerTableItem item3: biPointerT.biPointerTable){
//                        if(item1.tupleid == item3.objectid&&item3.deputyid == classid){
//                            for(ObjectTableItem item2: topt.objectTable){
//                                if(item2.tupleid == item3.deputyobjectid){
//                                    Tuple ituple = GetTuple(item2.blockid,item2.offset);
//                                    tpl.addTuple(ituple);
//                                }
//                            }
//                        }
//                    }
//
//                }
//            }
//
//        }
//        String[] name = new String[1];
//        name[0] = attrname;
//        int[] id = new int[1];
//        id[0] = 0;
////        PrintSelectResult(new SelectResult(tpl,name,id,attrtype));
//        return tpl;
//
//
//
//
//    }
//
//    //UPDATE Song SET type = ‘jazz’WHERE songId = 100;
//    //OPT_CREATE_UPDATE，Song，type，“jazz”，songId，=，100
//    //0                  1     2      3        4      5  6
//    private void Update(String[] p){
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
//
//        for(ObjectTableItem item3:topt.objectTable){
//            if(item3.classid == classid){
//                Tuple tuple = GetTuple(item3.blockid,item3.offset);
//                if(Condition(cattrtype,tuple,cattrid,p[6])){
//                    UpdatebyID(item3.tupleid,attrid,p[3].replace("\"",""));
//
//                }
//            }
//        }
//    }
//    private void UpdatebyID(int tupleid,int attrid,String value){
//        for(ObjectTableItem item: topt.objectTable){
//            if(item.tupleid ==tupleid){
//                Tuple tuple = GetTuple(item.blockid,item.offset);
//                tuple.tuple[attrid] = value;
//                UpateTuple(tuple,item.blockid,item.offset);
//                Tuple tuple1 = GetTuple(item.blockid,item.offset);
//                UpateTuple(tuple1,item.blockid,item.offset);
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
//    //INSERT INTO aa VALUES (1,2,"3");
//    //4,3,aa,1,2,"3"
//    private class OandB{
//        public List<ObjectTableItem> o= new ArrayList<>();
//        public List<BiPointerTableItem> b= new ArrayList<>();
//        public OandB(){}
//        public OandB(OandB oandB){
//            this.o = oandB.o;
//            this.b = oandB.b;
//        }
//
//        public OandB(List<ObjectTableItem> o, List<BiPointerTableItem> b) {
//            this.o = o;
//            this.b = b;
//        }
//    }
//    private Tuple GetTuple(int id, int offset) {
//
//        return mem.readTuple(id,offset);
//    }
//
//    private int[] InsertTuple(Tuple tuple){
//        return mem.writeTuple(tuple);
//    }
//
//    private void DeleteTuple(int id, int offset){
//        mem.deleteTuple();
//        return;
//    }
//
//    private void UpateTuple(Tuple tuple,int blockid,int offset){
//        mem.UpateTuple(tuple,blockid,offset);
//    }
//

    private void PrintSelectResult(SelectResult selectResult) {
        Intent intent = new Intent(context, PrintResult.class);
        Bundle bundle = new Bundle();
        bundle.putSerializable("tupleList", selectResult.getTpl());
        bundle.putStringArray("attrname", selectResult.getAttrname());
        bundle.putIntArray("attrid", selectResult.getAttrid());
        bundle.putStringArray("type", selectResult.getType());
        intent.putExtras(bundle);
        context.startActivity(intent);
    }

    private void PrintSelectResult(TupleList tpl, String[] attrname, int[] attrid, String[] type) {
        Intent intent = new Intent(context, PrintResult.class);
        Bundle bundle = new Bundle();
        bundle.putSerializable("tupleList", tpl);
        bundle.putStringArray("attrname", attrname);
        bundle.putIntArray("attrid", attrid);
        bundle.putStringArray("type", type);
        intent.putExtras(bundle);
        context.startActivity(intent);
    }

    public void PrintTab(){
        PrintTab(memConnect.getTopt(),memConnect.getSwitchingT(),memConnect.getDeputyt(),memConnect.getBiPointerT(),memConnect.getClasst());
    }

    private void PrintTab(ObjectTable topt,SwitchingTable switchingT,DeputyTable deputyt,BiPointerTable biPointerT,ClassTable classTable) {
        Intent intent = new Intent(context, ShowTable.class);
        Bundle bundle0 = new Bundle();
        bundle0.putSerializable("ObjectTable",topt);
        bundle0.putSerializable("SwitchingTable",switchingT);
        bundle0.putSerializable("DeputyTable",deputyt);
        bundle0.putSerializable("BiPointerTable",biPointerT);
        bundle0.putSerializable("ClassTable",classTable);
        intent.putExtras(bundle0);
        context.startActivity(intent);
    }


}
