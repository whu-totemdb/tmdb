package drz.tmdb.Transaction.Transactions.utils;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;

import drz.tmdb.memory.Tuple;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;

public class Formula {
    //表达式后续遍历处理的核心
    public ArrayList<Object> formulaExecute(Expression expression, SelectResult selectResult) throws TMDBException {
        ArrayList<Object> res=new ArrayList<>();
        switch ((expression.getClass().getSimpleName())){//根据表达式的类别，分别处理
            case "Addition": res=addition((Addition) expression,selectResult); break;
            case "Subtraction": res=subtraction((Subtraction) expression,selectResult); break;
            case "Division": res=division((Division) expression,selectResult); break;
            case "Modulo": res=modulo((Modulo) expression,selectResult); break;
            case "Multiplication": res= multiplication((Multiplication) expression,selectResult); break;
            case "LongValue": res=longValue((LongValue) expression,selectResult); break;
            case "Column": res=column((Column) expression,selectResult); break;
            case "Parenthesis": res=parenthesis((Parenthesis) expression,selectResult);break;
            case "StringValue" :res=StringValue((StringValue)expression,selectResult); break;
        }
        return res;
    }

    private ArrayList<Object> StringValue(StringValue expression, SelectResult selectResult) {

        String temp=expression.getValue();
        ArrayList<Object> res=new ArrayList<>();
        //返回全是该数字元素的列
        for(int i=0;i<selectResult.getTpl().tuplelist.size();i++){
            res.add(temp);
        }
        return res;
    }

    //最底层的算子之一，column，表示一个参数的信息
    public ArrayList<Object> column(Column column, SelectResult selectResult) throws TMDBException {
        //获取columnName
        String columnName=column.getColumnName();
        int index=-1;
        //找到这个column在selectResult中的对应index，在原始名和别名中都要进行寻找。
        for(int i=0;i<selectResult.getClassName().length;i++){
            if(selectResult.getAttrname()[i].equals(columnName)){
                if(column.getTable()==null
                        || column.getTable().getName().equals(selectResult.getClassName()[i])
                        || column.getTable().getName().equals(selectResult.getAlias()[i])){
                    index=i;
                    break;
                }
            }
        }
        if(index==-1) throw new TMDBException("找不到"+columnName);
        ArrayList<Object> res=new ArrayList<>();
        //正常的情况，从selectresult中提取出该列的数据
        for (Tuple tuple : selectResult.getTpl().tuplelist) {
            res.add(tuple.tuple[index]);
        }

        return res;
    }

    //加法的处理
    public ArrayList<Object> addition(Addition expression, SelectResult selectResult) throws TMDBException {
        //获取表达式左边元素
        ArrayList<Object> left=formulaExecute(expression.getLeftExpression(),selectResult);
        //获取表达式右边元素
        ArrayList<Object> right=formulaExecute(expression.getRightExpression(),selectResult);
        ArrayList<Object> res=new ArrayList<>();
        for(int i=0;i<left.size();i++){
            res.add(Double.parseDouble(String.valueOf(left.get(i)))+Double.parseDouble(String.valueOf(right.get(i))));
        }
        return res;
    }

    //减法处理
    public ArrayList<Object> subtraction(Subtraction expression, SelectResult selectResult) throws TMDBException {
        //获取表达式左边元素
        ArrayList<Object> left=formulaExecute(expression.getLeftExpression(),selectResult);
        //获取表达式右边元素
        ArrayList<Object> right=formulaExecute(expression.getRightExpression(),selectResult);
        ArrayList<Object> res=new ArrayList<>();
        for(int i=0;i<left.size();i++){
            res.add(Double.parseDouble(String.valueOf(left.get(i)))-Double.parseDouble(String.valueOf(right.get(i))));
        }
        return res;
    }

    //乘法处理
    public ArrayList<Object> multiplication(Multiplication expression,SelectResult selectResult) throws TMDBException {
        //获取表达式左边元素
        ArrayList<Object> left=formulaExecute(expression.getLeftExpression(),selectResult);
        //获取表达式右边元素
        ArrayList<Object> right=formulaExecute(expression.getRightExpression(),selectResult);
        ArrayList<Object> res=new ArrayList<>();
        for(int i=0;i<left.size();i++){
            res.add(Double.parseDouble(String.valueOf(left.get(i)))*Double.parseDouble(String.valueOf(right.get(i))));
        }
        return res;
    }

    //除法处理
    public ArrayList<Object> division(Division expression, SelectResult selectResult) throws TMDBException {
        //获取表达式左边元素
        ArrayList<Object> left=formulaExecute(expression.getLeftExpression(),selectResult);
        //获取表达式右边元素
        ArrayList<Object> right=formulaExecute(expression.getRightExpression(),selectResult);
        ArrayList<Object> res=new ArrayList<>();
        for(int i=0;i<left.size();i++){
            res.add(Double.parseDouble(String.valueOf(left.get(i)))/Double.parseDouble(String.valueOf(right.get(i))));
        }
        return res;
    }

    //余数处理
    public ArrayList<Object> modulo(Modulo expression, SelectResult selectResult) throws TMDBException {
        //获取表达式左边
        ArrayList<Object> left=formulaExecute(expression.getLeftExpression(),selectResult);
        //获取表达式右边
        ArrayList<Object> right=formulaExecute(expression.getRightExpression(),selectResult);
        ArrayList<Object> res=new ArrayList<>();
        for(int i=0;i<left.size();i++){
            res.add(Double.parseDouble((String) left.get(i))%Double.parseDouble((String) right.get(i)));
        }
        return res;
    }

    //小括号（）处理，直接返回内部的表达式
    public ArrayList<Object> parenthesis(Parenthesis expression, SelectResult selectResult) throws TMDBException {
        ArrayList<Object> res=formulaExecute(expression.getExpression(),selectResult);
        return res;
    }

    //数字元素处理
    public ArrayList<Object> longValue(LongValue value, SelectResult selectResult){
        double temp=(double)value.getValue();
        ArrayList<Object> res=new ArrayList<>();
        //返回全是该数字元素的列
        for(int i=0;i<selectResult.getTpl().tuplelist.size();i++){
            res.add(temp);
        }
        return res;
    }


}
