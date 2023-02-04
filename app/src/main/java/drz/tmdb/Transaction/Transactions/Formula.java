package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;

import drz.tmdb.Memory.Tuple;

public class Formula {
    public ArrayList<Object> formulaExecute(Expression expression,SelectResult selectResult){
        ArrayList<Object> res=new ArrayList<>();
        switch ((expression.getClass().getSimpleName())){
            case "Addition": res=addition((Addition) expression,selectResult); break;
            case "Subtraction": res=subtraction((Subtraction) expression,selectResult); break;
            case "Division": res=division((Division) expression,selectResult); break;
            case "Modulo": res=modulo((Modulo) expression,selectResult); break;
            case "Multiplication": res= multiplication((Multiplication) expression,selectResult); break;
            case "LongValue": res=longValue((LongValue) expression,selectResult); break;
            case "Column": res=column((Column) expression,selectResult); break;
            case "Parenthesis": res=parenthesis((Parenthesis) expression,selectResult); break;
        }
        return res;
    }

    public ArrayList<Object> column(Column column, SelectResult selectResult){
        String columnName=column.getColumnName();
        int index=-1;
        for(int i=0;i<selectResult.className.length;i++){
            if(selectResult.attrname[i].equals(columnName)){
                if(column.getTable()==null || column.getTable().toString().equals(selectResult.className[i])){
                    index=i;
                    break;
                }
            }
        }
        ArrayList<Object> res=new ArrayList<>();
        if(index>selectResult.className.length || index==-1){
            if(column.getColumnName().charAt(0)=='"'){
                for(int i=0;i<selectResult.tpl.tuplelist.size();i++){
                    res.add(columnName.replace("\"",""));
                }
            }
        }
        else {
            for (Tuple tuple : selectResult.tpl.tuplelist) {
                res.add(tuple.tuple[index]);
            }
        }
        return res;
    }

    public ArrayList<Object> addition(Addition expression, SelectResult selectResult){
        ArrayList<Object> left=formulaExecute(expression.getLeftExpression(),selectResult);
        ArrayList<Object> right=formulaExecute(expression.getRightExpression(),selectResult);
        ArrayList<Object> res=new ArrayList<>();
        for(int i=0;i<left.size();i++){
            res.add(Double.parseDouble(String.valueOf(left.get(i)))+Double.parseDouble(String.valueOf(right.get(i))));
        }
        return res;
    }

    public ArrayList<Object> subtraction(Subtraction expression, SelectResult selectResult){
        ArrayList<Object> left=formulaExecute(expression.getLeftExpression(),selectResult);
        ArrayList<Object> right=formulaExecute(expression.getRightExpression(),selectResult);
        ArrayList<Object> res=new ArrayList<>();
        for(int i=0;i<left.size();i++){
            res.add(Double.parseDouble(String.valueOf(left.get(i)))-Double.parseDouble(String.valueOf(right.get(i))));
        }
        return res;
    }

    public ArrayList<Object> multiplication(Multiplication expression,SelectResult selectResult){
        ArrayList<Object> left=formulaExecute(expression.getLeftExpression(),selectResult);
        ArrayList<Object> right=formulaExecute(expression.getRightExpression(),selectResult);
        ArrayList<Object> res=new ArrayList<>();
        for(int i=0;i<left.size();i++){
            res.add(Double.parseDouble(String.valueOf(left.get(i)))*Double.parseDouble(String.valueOf(right.get(i))));
        }
        return res;
    }

    public ArrayList<Object> division(Division expression, SelectResult selectResult){
        ArrayList<Object> left=formulaExecute(expression.getLeftExpression(),selectResult);
        ArrayList<Object> right=formulaExecute(expression.getRightExpression(),selectResult);
        ArrayList<Object> res=new ArrayList<>();
        for(int i=0;i<left.size();i++){
            res.add(Double.parseDouble(String.valueOf(left.get(i)))/Double.parseDouble(String.valueOf(right.get(i))));
        }
        return res;
    }

    public ArrayList<Object> modulo(Modulo expression, SelectResult selectResult){
        ArrayList<Object> left=formulaExecute(expression.getLeftExpression(),selectResult);
        ArrayList<Object> right=formulaExecute(expression.getRightExpression(),selectResult);
        ArrayList<Object> res=new ArrayList<>();
        for(int i=0;i<left.size();i++){
            res.add(Double.parseDouble((String) left.get(i))%Double.parseDouble((String) right.get(i)));
        }
        return res;
    }

    public ArrayList<Object> parenthesis(Parenthesis expression, SelectResult selectResult){
        ArrayList<Object> res=formulaExecute(expression.getExpression(),selectResult);
        return res;
    }

    public ArrayList<Object> longValue(LongValue value, SelectResult selectResult){
        double temp=(double)value.getValue();
        ArrayList<Object> res=new ArrayList<>();
        for(int i=0;i<selectResult.tpl.tuplelist.size();i++){
            res.add(temp);
        }
        return res;
    }


}
