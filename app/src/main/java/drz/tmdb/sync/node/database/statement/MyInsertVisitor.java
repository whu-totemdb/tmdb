package drz.tmdb.sync.node.database.statement;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.values.ValuesStatement;

import java.util.List;

public class MyInsertVisitor implements SelectVisitor {

    public int attrNum;

    public String[] attrType;

    public String[] value;

    @Override
    public void visit(PlainSelect plainSelect) {

    }

    @Override
    public void visit(SetOperationList setOperationList) {
        List<SelectBody> selectBodies = setOperationList.getSelects();
        selectBodies.get(0).accept(this);
    }

    @Override
    public void visit(WithItem withItem) {

    }

    @Override
    public void visit(ValuesStatement valuesStatement) {
        ExpressionList expressionList= (ExpressionList) valuesStatement.getExpressions();
        List<Expression> expressions=expressionList.getExpressions();

        for(int i=0;i<expressions.size();i++){
            RowConstructor rowConstructor= (RowConstructor) expressions.get(i);
            ExpressionList expressionList1=rowConstructor.getExprList();

            attrNum = expressionList1.getExpressions().size();
            attrType = new String[attrNum];
            value=new String[attrNum];

            for(int j=0;j<expressionList1.getExpressions().size();j++){
                if (expressionList1.getExpressions().get(j).getClass().getSimpleName().equals("Column")){
                    attrType[j] = "String";
                }
                else {
                    attrType[j] = expressionList1.getExpressions().get(j).getClass().getSimpleName();
                }

                value[j] = expressionList1.getExpressions().get(j).toString();
            }

        }
    }
}
