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

import drz.tmdb.Memory.Tuple;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.Select;
import drz.tmdb.Transaction.Transactions.Update;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;
import drz.tmdb.Transaction.Transactions.utils.SelectResult;

public class UpdateImpl implements Update {
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
}
