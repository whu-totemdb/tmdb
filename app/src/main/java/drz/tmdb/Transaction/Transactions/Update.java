package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.util.ArrayList;

public class Update {
    public ArrayList<Integer> update(Statement stmt){
        return execute((net.sf.jsqlparser.statement.update.Update) stmt);
    }

    //UPDATE Song SET type = ‘jazz’ WHERE songId = 100;
    //OPT_CREATE_UPDATE，Song，type，“jazz”，songId，=，100
    //0                  1     2      3        4      5  6
    public ArrayList<Integer> execute(net.sf.jsqlparser.statement.update.Update update){
        String updateTable=update.getTable().getName();
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
            case "GreaterThan" :
                GreaterThan greaterThan =(GreaterThan) where;
                p[2+2*updateSets.size()]=greaterThan.getLeftExpression().toString();
                p[3+2*updateSets.size()]=">";
                p[4+2*updateSets.size()]=greaterThan.getRightExpression().toString();
            case "MinorThan" :
                MinorThan minorThan =(MinorThan) where;
                p[2+2*updateSets.size()]=minorThan.getLeftExpression().toString();
                p[3+2*updateSets.size()]=">";
                p[4+2*updateSets.size()]=minorThan.getRightExpression().toString();
        }
        return new MemConnect().update(p);
    }
}
