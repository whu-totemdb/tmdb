package drz.tmdb.Transaction.Transactions.impl;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.util.ArrayList;
import java.util.List;

import drz.tmdb.Memory.SystemTable.BiPointerTableItem;
import drz.tmdb.Memory.SystemTable.ClassTableItem;
import drz.tmdb.Memory.SystemTable.DeputyTableItem;
import drz.tmdb.Memory.SystemTable.ObjectTableItem;
import drz.tmdb.Memory.SystemTable.SwitchingTableItem;
import drz.tmdb.Memory.Tuple;
import drz.tmdb.Memory.TupleList;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.Insert;
import drz.tmdb.Transaction.Transactions.Select;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;
import drz.tmdb.Transaction.Transactions.utils.SelectResult;

public class InsertImpl implements Insert {
    MemConnect memConnect=new MemConnect();

    public InsertImpl(MemConnect memConnect) {
        this.memConnect = memConnect;
    }

    public InsertImpl(){}

    public ArrayList<Integer> insert(Statement stmt) throws TMDBException {
        return execute((net.sf.jsqlparser.statement.insert.Insert)stmt);
    }

//    public boolean executor()
    public ArrayList<Integer> execute(net.sf.jsqlparser.statement.insert.Insert stmt) throws TMDBException {
//        ArrayList<ClassTableItem> classTableItems=memConnect.getSelectItem(table,columns);
        //获取表名
        Table table=stmt.getTable();
        //获取插入的column的名称
        List<Column> columns=stmt.getColumns();
        Select select=new SelectImpl();
        //insert后面的values是一个select节点，获取values或者其它类型select的值
        SelectResult selectResult=select.select(stmt.getSelect());
        //tuplelist就是SelectResult中实际存储tuple部分
        TupleList tupleList=selectResult.getTpl();
        //获取插入表的表名
        String tablename=table.getName();
        ArrayList<Integer> integers = new ArrayList<>();
        for(int i=0;i<tupleList.tuplenum;i++){
            Tuple tuple=tupleList.tuplelist.get(i);
            //创建memConnect中的tupleInsert需要的字符串数组
            String[] p=new String[tuple.tuple.length+3];
            p[0]="";
            p[1]=""+tuple.tuple.length;
            p[2]=tablename;
            //循环的将每个values（或者其它类型的）的值调用memConnect进行插入
            for(int j=0;j<tuple.tuple.length;j++){
                p[j+3]=tuple.tuple[j].toString();
            }
            integers.add(tupleInsert(p));
        }
        return integers;
    }

    //INSERT INTO aa VALUES (1,2,"3");
    //4,3,aa,1,2,"3"
    //0 1 2  3 4  5

    //INSERT INTO aa VALUES (1,2,"3");
    //4,3,aa,1,2,"3"
    //0 1 2  3 4  5
    public int tupleInsert(String[] p) throws TMDBException {
        int count = Integer.parseInt(p[1]);
        for(int o =0;o<count+3;o++){
            p[o] = p[o].replace("\"","");
        }

        String classname = p[2];
        Object[] tuple_ = new Object[count];

        int classid = -1;

        for(ClassTableItem item: memConnect.getClasst().classTable)
        {
            if(item.classname.equals(classname)){
                if(item.attrnum!=count) throw new TMDBException("插入参数长度不匹配实际参数长度");
                classid = item.classid;
                break;
            }
        }
        if(classid==-1) throw new TMDBException("找不到"+classname);



        for(int j = 0;j<count;j++){
            tuple_[j] = p[j+3];
        }

        Tuple tuple = new Tuple(tuple_);
        tuple.tupleHeader=count;
        int tupleid = memConnect.getTopt().maxTupleId++;
        memConnect.InsertTuple(tuple);
        memConnect.getTopt().objectTable.add(new ObjectTableItem(classid,tupleid));
        //向代理类加元组
        for(DeputyTableItem item: memConnect.getDeputyt().deputyTable){
            if(classid == item.originid){
                //判断代理规则

                String attrtype=null;
                int attrid=0;
                for(ClassTableItem item1: memConnect.getClasst().classTable){
                    if(item1.classid == classid&&item1.attrname.equals(item.deputyrule[0])) {
                        attrtype = item1.attrtype;
                        attrid = item1.attrid;
                        break;
                    }
                }

                if(memConnect.Condition(attrtype,tuple,attrid,item.deputyrule[2])){
                    String[] ss= p.clone();
                    String s1 = null;

                    for(ClassTableItem item2:memConnect.getClasst().classTable){
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
                    for(ClassTableItem item3 : memConnect.getClasst().classTable){
                        if(item3.classid == classid){
                            attrname1[k] = item3.attrname;
                            attrid1[k] = item3.attrid;
                            k++;

                            if (k ==count)
                                break;
                        }
                    }
                    for (int l = 0;l<count;l++) {
                        for (SwitchingTableItem item4 : memConnect.getSwitchingT().switchingTable) {
                            if (item4.attr.equals(attrname1[l])){
                                //判断被置换的属性是否是代理类的

                                for(ClassTableItem item8: memConnect.getClasst().classTable){
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
                    memConnect.getBiPointerT().biPointerTable.add(new BiPointerTableItem(classid,tupleid,item.deputyid,deojid));
                }
            }
        }
        return tupleid;
    }
}
