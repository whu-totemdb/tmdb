package edu.whu.tmdb.query.operations.utils;

import edu.whu.tmdb.query.operations.Exception.ErrorList;
import edu.whu.tmdb.storage.memory.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;


import edu.whu.tmdb.query.operations.Exception.TMDBException;

public class Formula {
    // 表达式后续遍历处理的核心
    public ArrayList<Object> formulaExecute(Expression expression, SelectResult selectResult) throws TMDBException {
        ArrayList<Object> dataList = new ArrayList<>();
        // 根据表达式的类别，分别处理
        String type = (expression.getClass().getSimpleName());
        switch (type) {
            case "Addition":
                dataList = addition((Addition) expression, selectResult);
                break;
            case "Subtraction":
                dataList = subtraction((Subtraction) expression, selectResult);
                break;
            case "Division":
                dataList = division((Division) expression, selectResult);
                break;
            case "Modulo":
                dataList = modulo((Modulo) expression, selectResult);
                break;
            case "Multiplication":
                dataList = multiplication((Multiplication) expression, selectResult);
                break;
            case "LongValue":
                dataList = longValue((LongValue) expression, selectResult);
                break;
            case "Column":
                dataList = column((Column) expression, selectResult);
                break;
            case "Parenthesis":
                dataList = parenthesis((Parenthesis) expression, selectResult);
                break;
            case "StringValue":
                dataList = StringValue((StringValue) expression, selectResult);
                break;
            case "SignedExpression":
                dataList = signedExpression((SignedExpression) expression, selectResult);
                break;
        }
        return dataList;
    }

    private ArrayList<Object> StringValue(StringValue expression, SelectResult selectResult) {

        String temp = expression.getValue();
        ArrayList<Object> res = new ArrayList<>();
        //返回全是该数字元素的列
        for (int i = 0; i < selectResult.getTpl().tuplelist.size(); i++) {
            res.add(temp);
        }
        return res;
    }

    // 最底层的算子之一，column，表示一个参数的信息（返回selectResult该属性的数据列表）
    public ArrayList<Object> column(Column column, SelectResult selectResult) throws TMDBException {
        // 获取columnName
        String columnName = column.getColumnName();
        int index = -1;
        // 找到这个column在selectResult中的对应index，在原始名和别名中都要进行寻找。
        for (int i = 0; i < selectResult.getClassName().length; i++) {
            if (!selectResult.getAttrname()[i].equals(columnName)) {
                continue;
            }
            if (column.getTable() == null || column.getTable().getName().equals(selectResult.getClassName()[i])
                    || column.getTable().getName().equals(selectResult.getAlias()[i])) {
                index = i;
                break;
            }
        }
        if (index == -1) {
            throw new TMDBException(ErrorList.COLUMN_NAME_DOES_NOT_EXIST, columnName);
        }
        ArrayList<Object> dataList = new ArrayList<>();
        String type = selectResult.getType()[index];
        for (Tuple tuple : selectResult.getTpl().tuplelist) {
            addToDataList(dataList, type, tuple.tuple[index]);
        }
        return dataList;
    }

    // 加法的处理
    public ArrayList<Object> addition(Addition expression, SelectResult selectResult) throws TMDBException {
        // 获取表达式左边元素
        ArrayList<Object> left = formulaExecute(expression.getLeftExpression(), selectResult);
        // 获取表达式右边元素
        ArrayList<Object> right = formulaExecute(expression.getRightExpression(), selectResult);
        ArrayList<Object> res = new ArrayList<>();
        for (int i = 0; i < left.size(); i++) {
            res.add(Double.parseDouble(String.valueOf(left.get(i))) + Double.parseDouble(String.valueOf(right.get(i))));
        }
        return res;
    }

    // 减法处理
    public ArrayList<Object> subtraction(Subtraction expression, SelectResult selectResult) throws TMDBException {
        // 获取表达式左边元素
        ArrayList<Object> left = formulaExecute(expression.getLeftExpression(), selectResult);
        // 获取表达式右边元素
        ArrayList<Object> right = formulaExecute(expression.getRightExpression(), selectResult);
        ArrayList<Object> res = new ArrayList<>();
        for (int i = 0; i < left.size(); i++) {
            res.add(Double.parseDouble(String.valueOf(left.get(i))) - Double.parseDouble(String.valueOf(right.get(i))));
        }
        return res;
    }

    // 乘法处理
    public ArrayList<Object> multiplication(Multiplication expression, SelectResult selectResult) throws TMDBException {
        // 获取表达式左边元素
        ArrayList<Object> left = formulaExecute(expression.getLeftExpression(), selectResult);
        // 获取表达式右边元素
        ArrayList<Object> right = formulaExecute(expression.getRightExpression(), selectResult);
        ArrayList<Object> res = new ArrayList<>();
        for (int i = 0; i < left.size(); i++) {
            res.add(Double.parseDouble(String.valueOf(left.get(i))) * Double.parseDouble(String.valueOf(right.get(i))));
        }
        return res;
    }

    // 除法处理
    public ArrayList<Object> division(Division expression, SelectResult selectResult) throws TMDBException {
        // 获取表达式左边元素
        ArrayList<Object> left = formulaExecute(expression.getLeftExpression(), selectResult);
        // 获取表达式右边元素
        ArrayList<Object> right = formulaExecute(expression.getRightExpression(), selectResult);
        ArrayList<Object> res = new ArrayList<>();
        for (int i = 0; i < left.size(); i++) {
            res.add(Double.parseDouble(String.valueOf(left.get(i))) / Double.parseDouble(String.valueOf(right.get(i))));
        }
        return res;
    }

    // 余数处理
    public ArrayList<Object> modulo(Modulo expression, SelectResult selectResult) throws TMDBException {
        // 获取表达式左边
        ArrayList<Object> left = formulaExecute(expression.getLeftExpression(), selectResult);
        // 获取表达式右边
        ArrayList<Object> right = formulaExecute(expression.getRightExpression(), selectResult);
        ArrayList<Object> res = new ArrayList<>();
        for (int i = 0; i < left.size(); i++) {
            res.add(Double.parseDouble((String) left.get(i)) % Double.parseDouble((String) right.get(i)));
        }
        return res;
    }

    // 小括号（）处理，直接返回内部的表达式
    public ArrayList<Object> parenthesis(Parenthesis expression, SelectResult selectResult) throws TMDBException {
        return formulaExecute(expression.getExpression(), selectResult);
    }

    // 正数处理
    public ArrayList<Object> longValue(LongValue value, SelectResult selectResult) {
        double temp = (double) value.getValue();
        ArrayList<Object> res = new ArrayList<>();
        // 返回全是该数字元素的列
        for (int i = 0; i < selectResult.getTpl().tuplelist.size(); i++) {
            res.add(temp);
        }
        return res;
    }

    // 负数处理
    public ArrayList<Object> signedExpression(SignedExpression value, SelectResult selectResult) {
        String sign = String.valueOf(value.getSign());
        double data = Double.parseDouble(sign + value.getExpression().toString());
        int size = selectResult.getTpl().tuplelist.size();
        ArrayList<Object> res = new ArrayList<>();
        // 返回全是该数字元素的列
        for (int i = 0; i < size; i++) {
            res.add(data);
        }
        return res;
    }

    private void addToDataList(ArrayList<Object> dataList, String type, Object obj) throws TMDBException {
        switch (type) {
            case "String":
                dataList.add(String.valueOf(obj));
                break;
            case "float":
                dataList.add(Float.parseFloat(String.valueOf(obj)));
                break;
            case "int":
                dataList.add(Integer.parseInt(String.valueOf(obj)));
                break;
            case "long":
                dataList.add(Long.parseLong(String.valueOf(obj)));
                break;
            case "short":
                dataList.add(Short.parseShort(String.valueOf(obj)));
                break;
            case "double":
                dataList.add(Double.parseDouble(String.valueOf(obj)));
                break;
            case "char":
                dataList.add(String.valueOf(obj));
            default:
                throw new TMDBException(ErrorList.TYPE_IS_NOT_SUPPORTED, type);
        }
    }
}
