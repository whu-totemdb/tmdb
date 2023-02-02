package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class Where {
    public SelectResult where(PlainSelect plainSelect,SelectResult selectResult){
        execute(plainSelect.getWhere(),selectResult);

        return selectResult;
    }

    public SelectResult execute(Expression expression, SelectResult selectResult){
        SelectResult res=new SelectResult();
        String a=expression.getClass().getSimpleName();
        switch (expression.getClass().getSimpleName()){
            case "OrExpression": res=orExpression((OrExpression) expression,selectResult); break;
            case "AndExpression": res=andExpression((AndExpression) expression,selectResult); break;
//            case "OrExpression": inExpression(expression,selectResult);
//            case "OrExpression": equalsToExpression(expression,selectResult);
        }
        return res;
    }

    public SelectResult andExpression(AndExpression expression, SelectResult selectResult){
        SelectResult res=new SelectResult();
        return res;
    }

    public SelectResult orExpression(OrExpression expression,SelectResult selectResult){
        SelectResult res=new SelectResult();
        return res;
    }

    public SelectResult inExpression(PlainSelect plainSelect,SelectResult selectResult){
        SelectResult res=new SelectResult();
        return res;
    }

    public SelectResult equalExpression(PlainSelect plainSelect,SelectResult selectResult){
        SelectResult res=new SelectResult();
        return res;
    }


}
