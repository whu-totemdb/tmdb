package drz.tmdb.Transaction.Transactions.impl;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;

import drz.tmdb.Memory.SystemTable.BiPointerTableItem;
import drz.tmdb.Memory.SystemTable.ClassTableItem;
import drz.tmdb.Memory.SystemTable.ObjectTableItem;
import drz.tmdb.Memory.SystemTable.SwitchingTableItem;
import drz.tmdb.Memory.Tuple;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.Select;
import drz.tmdb.Transaction.Transactions.Update;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;
import drz.tmdb.Transaction.Transactions.utils.SelectResult;

public class UpdateImpl implements Update {
    private MemConnect memConnect;

    public UpdateImpl(MemConnect memConnect) {
        this.memConnect = memConnect;
    }

    public UpdateImpl() {
    }

    public ArrayList<Integer> update(Statement stmt) throws JSQLParserException, TMDBException {
        return execute((net.sf.jsqlparser.statement.update.Update) stmt);
    }

    //UPDATE Song SET type = ‘jazz’ WHERE songId = 100;
    //OPT_CREATE_UPDATE，Song，type，“jazz”，songId，=，100
    //0                  1     2      3        4      5  6
    public ArrayList<Integer> execute(net.sf.jsqlparser.statement.update.Update update) throws JSQLParserException, TMDBException {
        String updateTable=update.getTable().getName();
        String sql="select * from " + updateTable + " where " + update.getWhere().toString() + ";";
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sql.getBytes());
        net.sf.jsqlparser.statement.select.Select parse = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil.parse(byteArrayInputStream);
        Select select=new SelectImpl();
        SelectResult selectResult = select.select(parse);
        MemConnect memConnect=new MemConnect();
        ArrayList<UpdateSet> updateSets = update.getUpdateSets();
        int[] indexs=new int[updateSets.size()];
        for (int i :
                indexs) {
            i=-1;
        }
        Object[] updateValue=new Object[updateSets.size()];
        for (int i = 0; i < updateSets.size(); i++) {
            UpdateSet updateSet = updateSets.get(i);
            for (int j = 0; j < selectResult.getAttrname().length; j++) {
                if(updateSet.getColumns().get(0).getColumnName().equals(selectResult.getAttrname()[j])){
                    indexs[i]=j;
                    updateValue[i]=updateSet.getExpressions().get(0).toString();
                    break;
                }
            }
            if(indexs[i]==-1) throw new TMDBException(updateSet.getColumns().get(0).getColumnName()+"在"+updateTable+"中不存在");
        }
        ArrayList<Integer> integers = new ArrayList<>();
        for(Tuple tuple:selectResult.getTpl().tuplelist){
            for (int i = 0; i < indexs.length; i++) {
                tuple.tuple[indexs[i]]=updateValue[i];
            }
            memConnect.UpateTuple(tuple,tuple.getTupleId());
            integers.add(tuple.getTupleId());
        }
        return integers;
//        Expression where = update.getWhere();
//        String[] p=new String[2+2*updateSets.size()+3];
//        p[0]="-1";
//        p[1]=updateTable;
//        for(int i=0;i<updateSets.size();i++){
//            UpdateSet updateSet = updateSets.get(i);
//            p[2+i*2]=updateSet.getColumns().get(0).getColumnName();
//            p[3+i*2]=updateSet.getExpressions().get(0).toString();
//        }
//        String temp=where.getClass().getSimpleName();
//        switch (temp){
//            case "EqualsTo" :
//                EqualsTo equals=(EqualsTo) where;
//                p[2+2*updateSets.size()]=equals.getLeftExpression().toString();
//                p[3+2*updateSets.size()]="=";
//                p[4+2*updateSets.size()]=equals.getRightExpression().toString();
//                break;
//            case "GreaterThan" :
//                GreaterThan greaterThan =(GreaterThan) where;
//                p[2+2*updateSets.size()]=greaterThan.getLeftExpression().toString();
//                p[3+2*updateSets.size()]=">";
//                p[4+2*updateSets.size()]=greaterThan.getRightExpression().toString();
//                break;
//            case "MinorThan" :
//                MinorThan minorThan =(MinorThan) where;
//                p[2+2*updateSets.size()]=minorThan.getLeftExpression().toString();
//                p[3+2*updateSets.size()]=">";
//                p[4+2*updateSets.size()]=minorThan.getRightExpression().toString();
//                break;
//            default:
//                break;
//        }
//        return new MemConnect().update(p);
    }

    public ArrayList<Integer> update(String[] p){
        String classname = p[1];
        String attrname = p[2];
        String cattrname = p[4];

        int classid = 0;
        int attrid = 0;
        String attrtype = null;
        int cattrid = 0;
        String cattrtype = null;
        for(ClassTableItem item : memConnect.getClasst().classTable){
            if (item.classname.equals(classname)){
                classid = item.classid;
                break;
            }
        }
        for(ClassTableItem item1 : memConnect.getClasst().classTable){
            if (item1.classid==classid&&item1.attrname.equals(attrname)){
                attrtype = item1.attrtype;
                attrid = item1.attrid;
            }
        }
        for(ClassTableItem item2 : memConnect.getClasst().classTable){
            if (item2.classid==classid&&item2.attrname.equals(cattrname)){
                cattrtype = item2.attrtype;
                cattrid = item2.attrid;
            }
        }


        ArrayList<Integer> integers = new ArrayList<>();
        for(ObjectTableItem item3: MemConnect.getTopt().objectTable){
            if(item3.classid == classid){
                Tuple tuple = memConnect.GetTuple(item3.tupleid);
                if(memConnect.Condition(cattrtype,tuple,cattrid,p[6])){
                    integers.add(item3.tupleid);
                    UpdatebyID(item3.tupleid,attrid,p[3].replace("\"",""));

                }
            }
        }
        return integers;
    }
    private void UpdatebyID(int tupleid,int attrid,String value){
        for(ObjectTableItem item: memConnect.getTopt().objectTable){
            if(item.tupleid ==tupleid){
                Tuple tuple = memConnect.GetTuple(item.tupleid);
                tuple.tuple[attrid] = value;
                memConnect.UpateTuple(tuple,item.tupleid);
                Tuple tuple1 = memConnect.GetTuple(item.tupleid);
                memConnect.UpateTuple(tuple1,item.tupleid);
            }
        }

        String attrname = null;
        for(ClassTableItem item2: memConnect.getClasst().classTable){
            if (item2.attrid == attrid){
                attrname = item2.attrname;
                break;
            }
        }
        for(BiPointerTableItem item1: memConnect.getBiPointerT().biPointerTable) {
            if (item1.objectid == tupleid) {


                for(ClassTableItem item4: memConnect.getClasst().classTable){
                    if(item4.classid==item1.deputyid){
                        String dattrname = item4.attrname;
                        int dattrid = item4.attrid;
                        for (SwitchingTableItem item5 : memConnect.getSwitchingT().switchingTable) {
                            String dswitchrule = null;
                            String dvalue = null;
                            if (item5.attr.equals(attrname) && item5.deputy.equals(dattrname)) {
                                dvalue = value;
                                if (Integer.parseInt(item5.rule) != 0) {
                                    dswitchrule = item5.rule;
                                    dvalue = Integer.toString(Integer.parseInt(dvalue) + Integer.parseInt(dswitchrule));
                                }
                                UpdatebyID(item1.deputyobjectid, dattrid, dvalue);
                                break;
                            }
                        }
                    }
                }
            }
        }

    }
}
