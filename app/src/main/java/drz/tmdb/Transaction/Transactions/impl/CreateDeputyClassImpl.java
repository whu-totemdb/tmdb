package drz.tmdb.Transaction.Transactions.impl;


import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.List;

import drz.tmdb.memory.SystemTable.BiPointerTableItem;
import drz.tmdb.memory.SystemTable.ClassTableItem;
import drz.tmdb.memory.SystemTable.DeputyTableItem;
import drz.tmdb.memory.SystemTable.SwitchingTableItem;
import drz.tmdb.memory.Tuple;
import drz.tmdb.Transaction.Transactions.CreateDeputyClass;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;
import drz.tmdb.Transaction.Transactions.utils.SelectResult;


public class CreateDeputyClassImpl implements CreateDeputyClass {
    private MemConnect memConnect;
    public CreateDeputyClassImpl(MemConnect memConnect){
        this.memConnect=memConnect;
    }

    public CreateDeputyClassImpl() {
    }

    public boolean createDeputyClass(Statement stmt) throws TMDBException {
        return execute((net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass) stmt);
    }
    //CREATE SELECTDEPUTY aa SELECT  b1+2 AS c1,b2 AS c2,b3 AS c3 FROM  bb WHERE t1="1" ;
    //2,3,aa,b1,1,2,c1,b2,0,0,c2,b3,0,0,c3,bb,t1,=,"1"
    //0 1 2  3  4 5 6  7  8 9 10 11 121314 15 16 17 18
    public boolean execute(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) throws TMDBException {
        //获取新创建代理类的名称
        String deputyClass=stmt.getDeputyClass().toString();
        int deputyType=getDeputyType(stmt);
        Select select=stmt.getSelect();
        //获取select语句的selectResult
        SelectResult selectResult=getSelectResult(select);
        if(memConnect.getClassId(deputyClass)!=-1){
            throw new TMDBException(deputyClass+" already exists");
        }
        return help(selectResult,deputyType,deputyClass);

//        int deputyClass1 = createDeputyClass(deputyClass, selectResult);

        //获取创建代理类的选择信息，即后面的select部分。
//        PlainSelect plainSelect= (PlainSelect) select.getSelectBody();
//        //获取select部分的selectItem
//        List<SelectItem> selectExpressionItemList=plainSelect.getSelectItems();
//        //获取select部分的fromItem
//        FromItem fromItem=plainSelect.getFromItem();




        //以下构建memConnect中的createSelectDeputy需要的参数。

//        String[] p=new String[3+4*selectExpressionItemList.size()+4];
//        p[0]="2";
//        p[1]=""+selectExpressionItemList.size();
//        p[2]=deputyClass;
//        for(int i=0;i<selectExpressionItemList.size();i++){
//            SelectExpressionItem selectExpressionItem= (SelectExpressionItem) selectExpressionItemList.get(i);
//            //这个部分将selectItem进行拆分，不同形式的selectItem需要进行不同处理，具体流程参考help
//            String[] temp=help(selectExpressionItem);
//            p[3+4*i]=temp[0];
//            p[4+4*i]=temp[1];
//            p[5+4*i]=temp[2];
//            p[6+4*i]=temp[3];
//        }
//        p[4*selectExpressionItemList.size()+3]=fromItem.toString();
//        if(fromItem.getAlias()!=null) {
//            p[3+4*selectExpressionItemList.size()]=fromItem.getAlias().getName();
//        }
//        else{
//            p[3+4*selectExpressionItemList.size()]=fromItem.toString();
//        }
//        //获取select的where部分
//
//        Expression where=plainSelect.getWhere();
//        String temp="";
//        if(where!=null){
//            temp=where.getClass().getSimpleName();
//        }
//        //获取where 的表达式形式
//        switch (temp){
//            case "EqualsTo" ://等于的处理
//                EqualsTo equals=(EqualsTo) where;
//                p[4+4*selectExpressionItemList.size()]=equals.getLeftExpression().toString();
//                p[5+4*selectExpressionItemList.size()]="=";
//                p[6+4*selectExpressionItemList.size()]=equals.getRightExpression().toString();
//                break;
//            case "GreaterThan" ://大于的处理
//                GreaterThan greaterThan =(GreaterThan) where;
//                p[4+4*selectExpressionItemList.size()]=greaterThan.getLeftExpression().toString();
//                p[5+4*selectExpressionItemList.size()]=">";
//                p[6+4*selectExpressionItemList.size()]=greaterThan.getRightExpression().toString();
//                break;
//            case "MinorThan" ://小于的处理
//                MinorThan minorThan =(MinorThan) where;
//                p[4+4*selectExpressionItemList.size()]=minorThan.getLeftExpression().toString();
//                p[5+4*selectExpressionItemList.size()]=">";
//                p[6+4*selectExpressionItemList.size()]=minorThan.getRightExpression().toString();
//                break;
//        }
//        return CreateSelectDeputy(p);
    }

    public boolean help(SelectResult selectResult, int deputyType, String deputyClass) throws TMDBException {
        int deputyId = createDeputyClass(deputyClass, selectResult, deputyType);
        insertDeputyTable(selectResult.getClassName(),deputyType,deputyId);
        insertTuple(selectResult,deputyId);
        return true;
    }


    private List<String> getColumns(Select select) {
        SelectImpl select1=new SelectImpl(memConnect);
        ArrayList<SelectItem> selectItemList=(ArrayList<SelectItem>)((PlainSelect)select.getSelectBody()).getSelectItems();
        HashMap<SelectItem,ArrayList<Column>> selectItemToColumn=select1.getSelectItemColumn(selectItemList);
        List<Column> selectColumnList=select1.getSelectColumnList(selectItemToColumn);
        List<String> res=new ArrayList<>();
        for (int i = 0; i < selectColumnList.size(); i++) {
            res.add(selectColumnList.get(i).getColumnName());
        }
        return res;
    }

    private int getDeputyType(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) {
        switch (stmt.getType().toLowerCase(Locale.ROOT)){
            case "selectdeputy": return 0;
            case "joindeputy": return 1;
            case "uniondeputy": return 2;
            case "groupbydeputy": return 3;
        }
        return -1;
    }

    //将创建deputyclass后面的selectResult拿到，用于后面的处理
     private SelectResult getSelectResult(Select select) throws TMDBException{
         SelectImpl select1 = new SelectImpl(this.memConnect);
        SelectResult selectResult=select1.select(select);
        return selectResult;
    }

    //第一步，创建代理类，代理类的classtype设置为de
    //同时，在switchingtable中插入源属性到代理属性的映射
    private int createDeputyClass(String deputyClassName, SelectResult selectResult, int deputyRule) throws TMDBException {
        memConnect.getClasst().maxid++;
        int classid = memConnect.getClasst().maxid;//代理类的id
        int count=selectResult.getAttrid().length;//代理类的长度
        for (int i = 0; i < selectResult.getAttrid().length; i++) {
            memConnect.getClasst().classTable.add(
                    new ClassTableItem(deputyClassName,
                                        classid,
                                        count,
                                        selectResult.getAttrid()[i],
                                        selectResult.getAttrname()[i],
                                        selectResult.getType()[i],
                                        "de",
                                        ""));
            String className=selectResult.getClassName()[i];
            int oriId=memConnect.getClassId(className);
            int oriAttrId=getOriAttrId(oriId,selectResult.getAlias()[i]);
            memConnect.getSwitchingT().switchingTable.add(
                    new SwitchingTableItem(oriId,oriAttrId,selectResult.getAlias()[i],classid,i,selectResult.getAttrname()[i],deputyRule+"")
            );
        }
        return classid;
    }

    private int getOriAttrId(int oriId, String alias) {
        for (int i = 0; i < memConnect.getClasst().classTable.size(); i++) {
            ClassTableItem classTableItem = memConnect.getClasst().classTable.get(i);
            if(classTableItem.classid==oriId && classTableItem.attrname.equals(alias)){
                return classTableItem.attrid;
            }
        }
        return -1;
    }

    //第二步，在deputytable中插入
    public void insertDeputyTable(String[] className,int deputyType, int deputyId) throws TMDBException {
        HashSet<String> collect = Arrays.stream(className).collect(Collectors.toCollection(HashSet::new));
        for (String s :
                collect) {
            int oriId=memConnect.getClassId(s);
            memConnect.getDeputyt().deputyTable.add(
                    new DeputyTableItem(oriId,deputyId,new String[]{deputyType+""})
            );
        }
    }

    //第三步，在ObjectTable中插入实际值
    private void insertTuple(SelectResult selectResult, int deputyId) throws TMDBException {
        InsertImpl insert=new InsertImpl(memConnect);
        List<String> columns= Arrays.asList(selectResult.getAttrname());
        for (int i = 0; i < selectResult.getTpl().tuplelist.size(); i++) {
            Tuple tuple=selectResult.getTpl().tuplelist.get(i);
            int deputyTupleId = insert.executeTuple(deputyId, columns, new Tuple(tuple.tuple));
            HashSet<Integer> origin = getOriginClass(selectResult);
            for (int o :
                    origin) {
                int classId=memConnect.getClassId(selectResult.getClassName()[o]);
                int oriTupleId=tuple.tupleIds[o];
                memConnect.getBiPointerT().biPointerTable.add(
                        new BiPointerTableItem(classId,oriTupleId,deputyId,deputyTupleId)
                );
            }
        }
    }
    
    private HashSet<Integer> getOriginClass(SelectResult selectResult){
        ArrayList<String> collect = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(ArrayList::new));
        HashSet<String> collect1 = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(HashSet::new));
        HashSet<Integer> res=new HashSet<>();
        for(String s:collect1){
            res.add(collect.indexOf(s));
        }
        return res;
    } 




//    public String[] help(SelectExpressionItem selectExpressionItem){
//        //返回一个长度为4的String数组
//        String[] res=new String[4];
//        //判断selectItem的表达式形式
//        String temp=selectExpressionItem.getExpression().getClass().getSimpleName();
//        switch (temp){
//            case "Addition"://加法的处理
//                Addition addition= (Addition) selectExpressionItem.getExpression();
//                res[0]=addition.getLeftExpression().toString();
//                res[1]="1";
//                res[2]=addition.getRightExpression().toString();
//                break;
//            case "Subtraction"://减法的处理
//                Subtraction subtraction= (Subtraction) selectExpressionItem.getExpression();
//                res[0]=subtraction.getLeftExpression().toString();
//                res[1]="0";
//                res[2]=subtraction.getRightExpression().toString();
//                break;
//        }
//        res[3]=selectExpressionItem.getAlias().getName();
//        return res;
//    }

//    public boolean CreateSelectDeputy(String[] p) throws TMDBException {
//        int count = Integer.parseInt(p[1]);
//        String classname = p[2];//代理类的名字
//        String bedeputyname = p[4*count+3];//代理的类的名字
//        memConnect.getClasst().maxid++;
//        int classid = memConnect.getClasst().maxid;//代理类的id
//        int bedeputyid = memConnect.getClassId(bedeputyname);//代理的类的id
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
//            for (ClassTableItem item:memConnect.getClasst().classTable) {
//                if (item.classname.equals(bedeputyname)&&item.attrname.equals(p[3+4*i])) {
//                    bedeputyid = item.classid;
//                    bedeputyattrid[i] = item.attrid;
//
//                    memConnect.getClasst().classTable.add(new ClassTableItem(classname, classid, count,attrid[i],attrname[i], item.attrtype,"de",""));
//                    //swi
////                    if(Integer.parseInt(p[4+4*i]) == 1){
////                        memConnect.getSwitchingT().switchingTable.add(new SwitchingTableItem(item.attrname,attrname[i],p[5+4*i]));
////                    }
////                    if(Integer.parseInt(p[4+4*i])==0){
////                        memConnect.getSwitchingT().switchingTable.add(new SwitchingTableItem(item.attrname,attrname[i],"0"));
////                    }
//                    break;
//                }
//            };
//        }
//
//        String[] con =new String[3];
//        con[0] = p[4+4*count];
//        con[1] = p[5+4*count];
//        con[2] = p[6+4*count];
//        memConnect.getDeputyt().deputyTable.add(new DeputyTableItem(bedeputyid,classid,con));
//
//        TupleList tpl= new TupleList();
//
//        int conid = 0;
//        String contype  = null;
//        for(ClassTableItem item3:memConnect.getClasst().classTable){
//            if(item3.attrname.equals(con[0])){
//                conid = item3.attrid;
//                contype = item3.attrtype;
//                break;
//            }
//        }
//        List<ObjectTableItem> obj = new ArrayList<>();
//        for(ObjectTableItem item2: memConnect.getTopt().objectTable){
//            if(item2.classid ==bedeputyid){
//                Tuple tuple = memConnect.GetTuple(item2.tupleid);
//                if(memConnect.Condition(contype,tuple,conid,con[2])){
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
//                    memConnect.getTopt().maxTupleId++;
//                    int tupid = memConnect.getTopt().maxTupleId;
//
//                    memConnect.InsertTuple(ituple);
//                    //topt.objectTable.add(new ObjectTableItem(classid,tupid,aa[0],aa[1]));
//                    obj.add(new ObjectTableItem(classid,tupid));
//
//                    //bi
//                    memConnect.getBiPointerT().biPointerTable.add(new BiPointerTableItem(bedeputyid,item2.tupleid,classid,tupid));
//
//                }
//            }
//        }
//        for(ObjectTableItem item6:obj) {
//            memConnect.getTopt().objectTable.add(item6);
//        }
//        return true;
//    }
}
