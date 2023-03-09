package drz.tmdb.Transaction.Transactions.impl;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;

import drz.tmdb.Memory.Tuple;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.Delete;
import drz.tmdb.Transaction.Transactions.Select;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;
import drz.tmdb.Transaction.Transactions.utils.SelectResult;

public class DeleteImpl implements Delete {
    public ArrayList<Integer> delete(Statement statement) throws JSQLParserException, TMDBException {
        return execute((net.sf.jsqlparser.statement.delete.Delete) statement);
    }

    public ArrayList<Integer> execute(net.sf.jsqlparser.statement.delete.Delete delete) throws JSQLParserException, TMDBException {
        //获取需要删除的表名
        Table table = delete.getTable();
        //获取delete中的where表达式
        Expression where = delete.getWhere();
        String sql="select * from " + table + " where " + where.toString() + ";";
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sql.getBytes());
        net.sf.jsqlparser.statement.select.Select parse = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil.parse(byteArrayInputStream);
        Select select=new SelectImpl();
        SelectResult selectResult = select.select(parse);
        MemConnect memConnect=new MemConnect();
        ArrayList<Integer> integers = new ArrayList<>();
        for(Tuple tuple:selectResult.getTpl().tuplelist){
            memConnect.DeleteTuple(tuple.getTupleId());
            integers.add(tuple.getTupleId());
        }
        return integers;
//        String[] p=new String[5];
//        p[0]="-1";
//        p[1]=table.getName();
//        //获取表达式的形式
//        String temp=where.getClass().getSimpleName();
//        switch (temp){
//            case "EqualsTo" ://等于的处理
//                EqualsTo equals=(EqualsTo) where;
//                p[2]=equals.getLeftExpression().toString();
//                p[3]="=";
//                p[4]=equals.getRightExpression().toString();
//                break;
//            case "GreaterThan" ://大于的处理
//                GreaterThan greaterThan =(GreaterThan) where;
//                p[2]=greaterThan.getLeftExpression().toString();
//                p[3]=">";
//                p[4]=greaterThan.getRightExpression().toString();
//                break;
//            case "MinorThan" ://小于的处理
//                MinorThan minorThan =(MinorThan) where;
//                p[2]=minorThan.getLeftExpression().toString();
//                p[3]=">";
//                p[4]=minorThan.getRightExpression().toString();
//                break;
//            default:
//                break;
//        }
//        return new MemConnect().delete(p);
    }
}
