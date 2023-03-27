package drz.tmdb.Transaction.Transactions.impl;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

import drz.tmdb.Memory.SystemTable.BiPointerTableItem;
import drz.tmdb.Memory.SystemTable.ClassTableItem;
import drz.tmdb.Memory.SystemTable.DeputyTableItem;
import drz.tmdb.Memory.SystemTable.ObjectTableItem;
import drz.tmdb.Memory.SystemTable.SwitchingTableItem;
import drz.tmdb.Memory.Tuple;
import drz.tmdb.Memory.TupleList;
import drz.tmdb.Transaction.Transactions.CreateDeputyClass;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;


public class CreateDeputyClassImpl implements CreateDeputyClass {
    private MemConnect memConnect;
    public CreateDeputyClassImpl(MemConnect memConnect){
        this.memConnect=memConnect;
    }

    public CreateDeputyClassImpl() {
    }

    public boolean createDeputyClass(Statement stmt){
        return execute((net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass) stmt);
    }
    //CREATE SELECTDEPUTY aa SELECT  b1+2 AS c1,b2 AS c2,b3 AS c3 FROM  bb WHERE t1="1" ;
    //2,3,aa,b1,1,2,c1,b2,0,0,c2,b3,0,0,c3,bb,t1,=,"1"
    //0 1 2  3  4 5 6  7  8 9 10 11 121314 15 16 17 18
    public boolean execute(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt){
        //获取新创建代理类的名称
        String deputyClass=stmt.getDeputyClass().toString();
        Select select=stmt.getSelect();
        //获取创建代理类的选择信息，即后面的select部分。
        PlainSelect plainSelect= (PlainSelect) select.getSelectBody();
        //获取select部分的selectItem
        List<SelectItem> selectExpressionItemList=plainSelect.getSelectItems();
        //获取select部分的fromItem
        FromItem fromItem=plainSelect.getFromItem();
        //以下构建memConnect中的createSelectDeputy需要的参数。
        String[] p=new String[3+4*selectExpressionItemList.size()+4];
        p[0]="2";
        p[1]=""+selectExpressionItemList.size();
        p[2]=deputyClass;
        for(int i=0;i<selectExpressionItemList.size();i++){
            SelectExpressionItem selectExpressionItem= (SelectExpressionItem) selectExpressionItemList.get(i);
            //这个部分将selectItem进行拆分，不同形式的selectItem需要进行不同处理，具体流程参考help
            String[] temp=help(selectExpressionItem);
            p[3+4*i]=temp[0];
            p[4+4*i]=temp[1];
            p[5+4*i]=temp[2];
            p[6+4*i]=temp[3];
        }
        p[3+4*selectExpressionItemList.size()]=fromItem.getAlias().getName();
        //获取select的where部分
        Expression where=plainSelect.getWhere();
        //获取where 的表达式形式
        String temp=where.getClass().getSimpleName();
        switch (temp){
            case "EqualsTo" ://等于的处理
                EqualsTo equals=(EqualsTo) where;
                p[4+4*selectExpressionItemList.size()]=equals.getLeftExpression().toString();
                p[5+4*selectExpressionItemList.size()]="=";
                p[6+4*selectExpressionItemList.size()]=equals.getRightExpression().toString();
            case "GreaterThan" ://大于的处理
                GreaterThan greaterThan =(GreaterThan) where;
                p[4+4*selectExpressionItemList.size()]=greaterThan.getLeftExpression().toString();
                p[5+4*selectExpressionItemList.size()]=">";
                p[6+4*selectExpressionItemList.size()]=greaterThan.getRightExpression().toString();
            case "MinorThan" ://小于的处理
                MinorThan minorThan =(MinorThan) where;
                p[4+4*selectExpressionItemList.size()]=minorThan.getLeftExpression().toString();
                p[5+4*selectExpressionItemList.size()]=">";
                p[6+4*selectExpressionItemList.size()]=minorThan.getRightExpression().toString();
        }
        return CreateSelectDeputy(p);
    }

    public String[] help(SelectExpressionItem selectExpressionItem){
        //返回一个长度为4的String数组
        String[] res=new String[4];
        //判断selectItem的表达式形式
        String temp=selectExpressionItem.getExpression().getClass().getSimpleName();
        switch (temp){
            case "Addition"://加法的处理
                Addition addition= (Addition) selectExpressionItem.getExpression();
                res[0]=addition.getLeftExpression().toString();
                res[1]="+";
                res[2]=addition.getRightExpression().toString();
            case "Subtraction"://减法的处理
                Subtraction subtraction= (Subtraction) selectExpressionItem.getExpression();
                res[0]=subtraction.getLeftExpression().toString();
                res[1]="+";
                res[2]=subtraction.getRightExpression().toString();
        }
        res[3]=selectExpressionItem.getAlias().getName();
        return res;
    }

    public boolean CreateSelectDeputy(String[] p) {
        int count = Integer.parseInt(p[1]);
        String classname = p[2];//代理类的名字
        String bedeputyname = p[4*count+3];//代理的类的名字
        memConnect.getClasst().maxid++;
        int classid = memConnect.getClasst().maxid;//代理类的id
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

            for (ClassTableItem item:memConnect.getClasst().classTable) {
                if (item.classname.equals(bedeputyname)&&item.attrname.equals(p[3+4*i])) {
                    bedeputyid = item.classid;
                    bedeputyattrid[i] = item.attrid;

                    memConnect.getClasst().classTable.add(new ClassTableItem(classname, classid, count,attrid[i],attrname[i], item.attrtype,"de",""));
                    //swi
                    if(Integer.parseInt(p[4+4*i]) == 1){
                        memConnect.getSwitchingT().switchingTable.add(new SwitchingTableItem(item.attrname,attrname[i],p[5+4*i]));
                    }
                    if(Integer.parseInt(p[4+4*i])==0){
                        memConnect.getSwitchingT().switchingTable.add(new SwitchingTableItem(item.attrname,attrname[i],"0"));
                    }
                    break;
                }
            };
        }

        String[] con =new String[3];
        con[0] = p[4+4*count];
        con[1] = p[5+4*count];
        con[2] = p[6+4*count];
        memConnect.getDeputyt().deputyTable.add(new DeputyTableItem(bedeputyid,classid,con));

        TupleList tpl= new TupleList();

        int conid = 0;
        String contype  = null;
        for(ClassTableItem item3:memConnect.getClasst().classTable){
            if(item3.attrname.equals(con[0])){
                conid = item3.attrid;
                contype = item3.attrtype;
                break;
            }
        }
        List<ObjectTableItem> obj = new ArrayList<>();
        for(ObjectTableItem item2: memConnect.getTopt().objectTable){
            if(item2.classid ==bedeputyid){
                Tuple tuple = memConnect.GetTuple(item2.tupleid);
                if(memConnect.Condition(contype,tuple,conid,con[2])){
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

                    memConnect.getTopt().maxTupleId++;
                    int tupid = memConnect.getTopt().maxTupleId;

                    memConnect.InsertTuple(ituple);
                    //topt.objectTable.add(new ObjectTableItem(classid,tupid,aa[0],aa[1]));
                    obj.add(new ObjectTableItem(classid,tupid));

                    //bi
                    memConnect.getBiPointerT().biPointerTable.add(new BiPointerTableItem(bedeputyid,item2.tupleid,classid,tupid));

                }
            }
        }
        for(ObjectTableItem item6:obj) {
            memConnect.getTopt().objectTable.add(item6);
        }
        return true;
    }
}
