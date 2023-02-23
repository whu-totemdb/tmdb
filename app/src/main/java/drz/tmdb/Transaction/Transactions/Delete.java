package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

public class Delete {
    public boolean delete(Statement statement){
        return execute((net.sf.jsqlparser.statement.delete.Delete) statement);
    }

    public boolean execute(net.sf.jsqlparser.statement.delete.Delete delete){
        //获取需要删除的表名
        Table table = delete.getTable();
        //获取delete中的where表达式
        Expression where = delete.getWhere();
        String[] p=new String[5];
        p[0]="-1";
        p[1]=table.getName();
        //获取表达式的形式
        String temp=where.getClass().getSimpleName();
        switch (temp){
            case "EqualsTo" ://等于的处理
                EqualsTo equals=(EqualsTo) where;
                p[2]=equals.getLeftExpression().toString();
                p[3]="=";
                p[4]=equals.getRightExpression().toString();
            case "GreaterThan" ://大于的处理
                GreaterThan greaterThan =(GreaterThan) where;
                p[2]=greaterThan.getLeftExpression().toString();
                p[3]=">";
                p[4]=greaterThan.getRightExpression().toString();
            case "MinorThan" ://小于的处理
                MinorThan minorThan =(MinorThan) where;
                p[2]=minorThan.getLeftExpression().toString();
                p[3]=">";
                p[4]=minorThan.getRightExpression().toString();
        }
        return new MemConnect().delete(p);
    }
}
