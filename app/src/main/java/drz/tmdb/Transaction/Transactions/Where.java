package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import drz.tmdb.Memory.Tuple;
import drz.tmdb.Memory.TupleList;

public class Where {
    Formula formula=new Formula();
    public SelectResult where(PlainSelect plainSelect,SelectResult selectResult){
        execute(plainSelect.getWhere(),selectResult);

        return selectResult;
    }

    public SelectResult execute(Expression expression,SelectResult selectResult){
        SelectResult res=new SelectResult();
        if(selectResult.tpl.tuplelist.isEmpty()) return selectResult;
        String a=expression.getClass().getSimpleName();
        switch (expression.getClass().getSimpleName()){
            case "OrExpression": res=orExpression((OrExpression) expression,selectResult); break;
            case "AndExpression": res=andExpression((AndExpression) expression,selectResult); break;
            case "INExpression": res=inExpression((InExpression) expression,selectResult); break;
            case "EqualsTo": res=equalsToExpression((EqualsTo) expression,selectResult); break;
            case "MinorThan": res=minorThan((MinorThan) expression,selectResult);
        }
        return res;
    }


    public SelectResult andExpression(AndExpression expression, SelectResult selectResult){
        Expression left=expression.getLeftExpression();
        Expression right=expression.getRightExpression();
        SelectResult selectResult1=execute(left,selectResult);
        SelectResult selectResult2=execute(right,selectResult);
        HashSet<Tuple> selectResultSet1=getTupleSet(selectResult1);
        HashSet<Tuple> selectResultSet2=getTupleSet(selectResult2);
        HashSet<Tuple> overlap=new HashSet<>();
        for(Tuple tuple:selectResultSet2){
            if(selectResultSet1.contains(tuple)) overlap.add(tuple);
        }
        return getSelectResultFromSet(selectResult,overlap);
    }

    public SelectResult orExpression(OrExpression expression,SelectResult selectResult){
        Expression left=expression.getLeftExpression();
        Expression right=expression.getRightExpression();
        SelectResult selectResult1=execute(left,selectResult);
        SelectResult selectResult2=execute(right,selectResult);
        HashSet<Tuple> selectResultSet1=getTupleSet(selectResult1);
        HashSet<Tuple> selectResultSet2=getTupleSet(selectResult2);
        for(Tuple tuple:selectResultSet2){
            selectResultSet1.add(tuple);
        }
        return getSelectResultFromSet(selectResult,selectResultSet1);
    }

    public SelectResult inExpression(InExpression expression, SelectResult selectResult){
        Expression left=expression.getLeftExpression();
        Expression right=expression.getRightExpression();
        SelectResult selectResult1=execute(left,selectResult);
        SelectResult selectResult2=execute(right,selectResult);
        HashSet<Tuple> selectResultSet1=getTupleSet(selectResult1);
        HashSet<Tuple> selectResultSet2=getTupleSet(selectResult2);
        SelectResult res=new SelectResult();
        return res;
    }

    public SelectResult equalsToExpression(EqualsTo expression,SelectResult selectResult){
        ArrayList<Object> left=formula.formulaExecute(expression.getLeftExpression(),selectResult);
        ArrayList<Object> right=formula.formulaExecute(expression.getRightExpression(),selectResult);
        HashSet<Tuple> set=new HashSet<>();
        for(int i=0;i<left.size();i++){
            String tempLeft=transType(left.get(i));
            String tempRight=transType(right.get(i));
            if(tempLeft.equals(tempRight)) set.add(selectResult.tpl.tuplelist.get(i));
        }
        return getSelectResultFromSet(selectResult,set);
    }

    public SelectResult minorThan(MinorThan expression,SelectResult selectResult){
        ArrayList<Object> left=formula.formulaExecute(expression.getLeftExpression(),selectResult);
        ArrayList<Object> right=formula.formulaExecute(expression.getRightExpression(),selectResult);
        HashSet<Tuple> set=new HashSet<>();
        for(int i=0;i<left.size();i++){
            String tempLeft=transType(left.get(i));
            String tempRight=transType(right.get(i));
            if(tempLeft.compareTo(tempRight)<0) set.add(selectResult.tpl.tuplelist.get(i));
        }
        return getSelectResultFromSet(selectResult,set);
    }
    
    

    public HashSet<Tuple> getTupleSet(SelectResult selectResult){
        HashSet<Tuple> set=new HashSet<>();
        for(Tuple tuple:selectResult.tpl.tuplelist){
            set.add(tuple);
        }
        return set;
    }

    public SelectResult getSelectResultFromSet(SelectResult selectResult,HashSet<Tuple> set){
        TupleList tupleList=new TupleList();
        List<Tuple> tupleList1=new ArrayList<>();
        for(Tuple tuple:set){
            tupleList1.add(tuple);
        }
        tupleList.tuplelist=tupleList1;
        selectResult.tpl=tupleList;
        return selectResult;
    }

    public String transType(Object obj){
        switch(obj.getClass().getSimpleName()){
            case "String":
                boolean flag=false;
                try{
                    Double temp=Double.parseDouble(String.valueOf(obj));
                    flag=true;
                }
                catch(Throwable throwable){}
                if(flag==true) return String.valueOf(Double.parseDouble(String.valueOf(obj)));
                else return (String)obj;
            case "Float": return String.valueOf((double) obj);
            case "Double": return String.valueOf(obj);
            case "Integer": return String.valueOf((double) obj);
            case "Long": return String.valueOf((double) obj);
            case "Character": return String.valueOf(obj);
            case "Short": return String.valueOf((double) obj);
            default: return "";
        }
    }
}
