package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.util.SelectUtils;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import drz.tmdb.Memory.Tuple;

public class Update {
    public ArrayList<Integer> update(Statement stmt) throws JSQLParserException {
        return execute((net.sf.jsqlparser.statement.update.Update) stmt);
    }

    //UPDATE Song SET type = ‘jazz’ WHERE songId = 100;
    //OPT_CREATE_UPDATE，Song，type，“jazz”，songId，=，100
    //0                  1     2      3        4      5  6
    public ArrayList<Integer> execute(net.sf.jsqlparser.statement.update.Update update) throws JSQLParserException {
        String updateTable=update.getTable().getName();
        String sql="select * from " + updateTable + " where " + update.getWhere().toString() + ";";
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sql.getBytes());
        net.sf.jsqlparser.statement.select.Select parse = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil.parse(byteArrayInputStream);
        SelectResult selectResult = new Select().select(parse);
        MemConnect memConnect=new MemConnect();
        for(Tuple tuple:selectResult.tpl.tuplelist){
//            memConnect.DeleteTuple(tuple.getTupleId());
//            memConnect.InsertTuple(tuple);
        }
        ArrayList<UpdateSet> updateSets = update.getUpdateSets();
        Expression where = update.getWhere();
        String[] p=new String[2+2*updateSets.size()+3];
        p[0]="-1";
        p[1]=updateTable;
        for(int i=0;i<updateSets.size();i++){
            UpdateSet updateSet = updateSets.get(i);
            p[2+i*2]=updateSet.getColumns().get(0).getColumnName();
            p[3+i*2]=updateSet.getExpressions().get(0).toString();
        }
        String temp=where.getClass().getSimpleName();
        switch (temp){
            case "EqualsTo" :
                EqualsTo equals=(EqualsTo) where;
                p[2+2*updateSets.size()]=equals.getLeftExpression().toString();
                p[3+2*updateSets.size()]="=";
                p[4+2*updateSets.size()]=equals.getRightExpression().toString();
                break;
            case "GreaterThan" :
                GreaterThan greaterThan =(GreaterThan) where;
                p[2+2*updateSets.size()]=greaterThan.getLeftExpression().toString();
                p[3+2*updateSets.size()]=">";
                p[4+2*updateSets.size()]=greaterThan.getRightExpression().toString();
                break;
            case "MinorThan" :
                MinorThan minorThan =(MinorThan) where;
                p[2+2*updateSets.size()]=minorThan.getLeftExpression().toString();
                p[3+2*updateSets.size()]=">";
                p[4+2*updateSets.size()]=minorThan.getRightExpression().toString();
                break;
            default:
                break;
        }
        return new MemConnect().update(p);
    }
}
