package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import drz.tmdb.Memory.Tuple;
import drz.tmdb.Memory.TupleList;
import drz.tmdb.Transaction.SystemTable.ClassTableItem;

//1、from子句组装来自不同数据源的数据；
//2、where子句基于指定的条件对记录行进行筛选；
//3、group by子句将数据划分为多个分组；
//4、使用聚集函数进行计算；
//5、使用having子句筛选分组；
//6、计算所有的表达式；
//7、select 的字段；
//8、使用order by对结果集进行排序。
public class Select {

    private static MemConnect memConnect=new MemConnect();
    public Select(){}

    public SelectResult select(net.sf.jsqlparser.statement.select.Select stmt){
        stmt=(net.sf.jsqlparser.statement.select.Select)stmt;
        SelectResult res=new SelectResult();
        if(((net.sf.jsqlparser.statement.select.Select) stmt).getSelectBody().getClass().getSimpleName().equals("SetOperationList")){
            SetOperationList setOperationList= (SetOperationList) ((net.sf.jsqlparser.statement.select.Select) stmt).getSelectBody();
            return setOperation(setOperationList);
        }
        else if(((net.sf.jsqlparser.statement.select.Select) stmt).getSelectBody().getClass().getSimpleName().equals("PlainSelect")){
            res=plainSelect(((net.sf.jsqlparser.statement.select.Select) stmt).getSelectBody());
        }
        return res;
    }

    public SelectResult plainSelect(SelectBody stmt){
//        if(plainSelect)
        if(stmt.getClass().getSimpleName().equals("ValuesStatement")) return values((ValuesStatement) stmt);
        PlainSelect plainSelect= (PlainSelect) stmt;
        if(plainSelect.isDeputySelect()==true){
            return plainSelect(stmt);
        }
        SelectResult selectResult=from(plainSelect);
        selectResult=elicit(plainSelect,selectResult);
        return selectResult;
    }

    public TupleList normalSelect(FromItem fromItem){
        TupleList tupleList=memConnect.getTable(fromItem);
        return tupleList;
    }

    public SelectResult elicit(PlainSelect plainSelect,SelectResult selectResult){
        ArrayList<SelectItem> selectItemList= (ArrayList<SelectItem>) plainSelect.getSelectItems();
        HashMap<SelectItem,ArrayList<Column>> selectItemToColumn=getSelectItemColumn(selectItemList);
        List<Column> selectColumnList=getSelectColumnList(selectItemToColumn);
        ArrayList<Integer> elicitIndexList=getElicitIndexList(selectColumnList,selectResult);
        return getElicitSelectResult(elicitIndexList,selectResult);
    }

    public ArrayList<Integer> getElicitIndexList(List<Column> selectColumnList,SelectResult selectResult){
        ArrayList<Integer> elicitIndexList=new ArrayList<>();
        for(Column column:selectColumnList){
            for(int i=0;i<selectResult.attrname.length;i++){
                if(selectResult.attrname[i].equals(column.getColumnName())) {
                    if (column.getTable()==null || selectResult.className[i].equals(column.getTable().toString())) {
                        elicitIndexList.add(i);
                        break;
                    }
                }
            }
        }
        return elicitIndexList;
    }

    public SelectResult getElicitSelectResult(ArrayList<Integer> elicitIndexList,SelectResult selectResult){
        SelectResult res=new SelectResult();
        res.className=new String[elicitIndexList.size()];
        res.attrname=new String[elicitIndexList.size()];
        res.attrid=new int[elicitIndexList.size()];
        res.type=new String[elicitIndexList.size()];
        for(int i=0;i<elicitIndexList.size();i++){
            res.className[i]=selectResult.className[elicitIndexList.get(i)];
            res.attrid[i]=selectResult.attrid[elicitIndexList.get(i)];
            res.attrname[i]=selectResult.attrname[elicitIndexList.get(i)];
            res.type[i]=selectResult.type[elicitIndexList.get(i)];
        }
        TupleList elicitTupleList=new TupleList();
        for(Tuple tuple:selectResult.tpl.tuplelist) {
            Tuple elicitTuple=new Tuple();
            Object[] temp=new Object[elicitIndexList.size()];
            for(int i=0;i<elicitIndexList.size();i++) {
                temp[i]=tuple.tuple[elicitIndexList.get(i)];
            }
            elicitTuple.tuple=temp;
            elicitTupleList.addTuple(elicitTuple);
        }

        res.tpl=elicitTupleList;
        return res;
    }

    public SelectResult from(PlainSelect plainSelect){
        FromItem fromItem=plainSelect.getFromItem();
        TupleList tupleList=memConnect.getTable(fromItem);
        ArrayList<ClassTableItem> classTableItemList=memConnect.getSelectItem(fromItem);
        SelectResult selectResult=getSelectResult(classTableItemList,tupleList);
        if(!(plainSelect.getJoins() ==null)){
            for(Join join:plainSelect.getJoins()){
                ArrayList<ClassTableItem> tempClassTableItem=memConnect.getSelectItem(join.getRightItem());
                TupleList tempTupleList=memConnect.getTable(join.getRightItem());
                SelectResult tempSelectResult=getSelectResult(tempClassTableItem,tempTupleList);
                tupleList=join(selectResult,tempSelectResult,join);
                classTableItemList=getJoinClassTableItem(classTableItemList,tempClassTableItem,join);
                selectResult=getSelectResult(classTableItemList,tupleList);
            }
        }
        return selectResult;
    }

    public SelectResult deputySelect(PlainSelect plainSelect){
        ArrayList<SelectItem> selectItemList=(ArrayList<SelectItem>)plainSelect.getSelectItems();
        FromItem fromItem=plainSelect.getFromItem();
        TupleList tupleList=memConnect.getTable(fromItem);
        HashMap<SelectItem,ArrayList<Column>> selectItemToColumn=getSelectItemColumn(selectItemList);
        List<Column> selectColumnList=getSelectColumnList(selectItemToColumn);
        ArrayList<ClassTableItem> classTableItemList=memConnect.getSelectItem(fromItem,selectColumnList);
        return getSelectResult(classTableItemList,tupleList);
    }

    public SelectResult setOperation(SetOperationList setOperationList){
        SelectResult selectResult=new SelectResult();
        List<SelectBody> plainSelectList=setOperationList.getSelects();
        List<SetOperation> setOperationList1=setOperationList.getOperations();
        selectResult=plainSelect(plainSelectList.get(0));
        for(int i=1;i<plainSelectList.size();i++){
            SelectResult tempResult=plainSelect((PlainSelect) plainSelectList.get(i));
            selectResult=Operate(selectResult,tempResult,setOperationList1.get(i-1));
        }
        return selectResult;
    }

    public SelectResult Operate(SelectResult selectResult1, SelectResult selectResult2, SetOperation setOperation){
        switch (setOperation.toString()){
            case "UNION": return union(selectResult1,selectResult2);
            case "INTERSECT": return intersect(selectResult1,selectResult2);
            case "EXCEPT": return except(selectResult1,selectResult2);
            case "MINUS": return minus(selectResult1,selectResult2);
        }
        return null;
    }

    public SelectResult union(SelectResult selectResult1,SelectResult selectResult2){
        return selectResult1;
    }

    public SelectResult intersect(SelectResult selectResult1,SelectResult selectResult2){
        return selectResult1;
    }

    public SelectResult except(SelectResult selectResult1,SelectResult selectResult2){
        return selectResult1;
    }

    public SelectResult minus(SelectResult selectResult1,SelectResult selectResult2){
        return selectResult1;
    }

    public SelectResult values(ValuesStatement valuesStatement){
        ExpressionList expressionList= (ExpressionList) valuesStatement.getExpressions();
        List<Expression> expressions=expressionList.getExpressions();
        TupleList tupleList=new TupleList();
        for(int i=0;i<expressions.size();i++){
            RowConstructor rowConstructor= (RowConstructor) expressions.get(i);
            ExpressionList expressionList1=rowConstructor.getExprList();
            Object[] tuple=new Object[expressionList1.getExpressions().size()];
            for(int j=0;j<expressionList1.getExpressions().size();j++){
                tuple[j]=expressionList1.getExpressions().get(j).toString();
            }
            tupleList.addTuple(new Tuple(tuple));
        }
        ArrayList<ClassTableItem> classTableItemArrayList=new ArrayList<>();
        for(int i=0;i<tupleList.tuplelist.get(0).tuple.length;i++){
            ClassTableItem classTableItem=new ClassTableItem("", -1, tupleList.tuplelist.get(0).tuple.length,i, "attr"+i,tupleList.tuplelist.get(0).tuple.getClass().getSimpleName(),"");
            classTableItemArrayList.add(classTableItem);
        }
        return getSelectResult(classTableItemArrayList,tupleList);
    }

    public TupleList join(SelectResult left,SelectResult right,Join join){
        TupleList leftTupleList=left.tpl;
        TupleList rightTupleList=right.tpl;
        LinkedList<Expression> expressionLinkedList=(LinkedList<Expression>)join.getOnExpressions();
        EqualsTo equals=(EqualsTo) expressionLinkedList.get(0);
        Column leftExpression=(Column) equals.getLeftExpression();
        Column rightExpression=(Column) equals.getRightExpression();
        int leftIndex=0;
        int rightIndex=0;
        for(int i=0;i<left.attrname.length;i++){
            if(leftExpression.getColumnName().equals(left.attrname[i])){
                leftIndex=i;
                break;
            }
        }
        for(int i=0;i<right.attrname.length;i++){
            if(rightExpression.getColumnName().equals(right.attrname[i])){
                rightIndex=i;
                break;
            }
        }
        if(join.isNatural() || join.isInner()){
            leftTupleList=naturalJoin(leftTupleList,rightTupleList,leftIndex,rightIndex);
        }
        if(join.isLeft()){
            leftTupleList=leftJoin(leftTupleList,rightTupleList,leftIndex,rightIndex);
        }
        else if(join.isRight()){
            leftTupleList=leftJoin(rightTupleList,leftTupleList,rightIndex,leftIndex);
        }

        return leftTupleList;
    }

    public TupleList naturalJoin(TupleList left,TupleList right,int leftIndex, int rightIndex){
        TupleList tupleList=new TupleList();
        for(Tuple leftTuple:left.tuplelist){
            for(Tuple rightTuple:right.tuplelist){
                if(leftTuple.tuple[leftIndex].equals(rightTuple.tuple[rightIndex])){
                    Tuple tempTuple=new Tuple();
                    int newLength=leftTuple.tuple.length+rightTuple.tuple.length-1;
                    Object[] tuple=new Object[newLength];
                    for(int i=0;i<leftTuple.tuple.length;i++){
                        tuple[i]=leftTuple.tuple[i];
                    }
                    for(int i=leftTuple.tuple.length;i<rightTuple.tuple.length;i++){
                        if(i-leftTuple.tuple.length==rightIndex) continue;
                        if(i<leftIndex+rightIndex) tuple[i]=rightTuple.tuple[i-leftTuple.tuple.length];
                        else {
                            tuple[i-1]=rightTuple.tuple[i-leftTuple.tuple.length];
                        }
                    }
                    tempTuple.tuple=tuple;
                    tupleList.addTuple(tempTuple);
                }
            }
        }
        return tupleList;
    }

    public TupleList leftJoin(TupleList left,TupleList right,int leftIndex, int rightIndex){
        TupleList tupleList=new TupleList();
        for(Tuple leftTuple:left.tuplelist){
            for(Tuple rightTuple:right.tuplelist){
                if(leftTuple.tuple[leftIndex].equals(rightTuple.tuple[rightIndex])){
                    Tuple tempTuple=new Tuple();
                    int newLength=leftTuple.tuple.length+rightTuple.tuple.length-1;
                    Object[] tuple=new Object[newLength];
                    for(int i=0;i<leftTuple.tuple.length;i++){
                        tuple[i]=leftTuple.tuple[i];
                    }
                    for(int i=leftTuple.tuple.length;i<leftTuple.tuple.length+rightTuple.tuple.length;i++){
                        if(i-leftTuple.tuple.length==rightIndex) continue;
                        if(i<leftIndex+rightIndex) tuple[i]=rightTuple.tuple[i-leftTuple.tuple.length];
                        else {
                            tuple[i-1]=rightTuple.tuple[i-leftTuple.tuple.length];
                        }
                    }
                    tempTuple.tuple=tuple;
                    tupleList.addTuple(tempTuple);
                }
                else{
                    Tuple tempTuple=new Tuple();
                    int newLength=leftTuple.tuple.length+rightTuple.tuple.length-1;
                    Object[] tuple=new Object[newLength];
                    for(int i=0;i<leftTuple.tuple.length;i++){
                        tuple[i]=leftTuple.tuple[i];
                    }
                    tempTuple.tuple=tuple;
                    tupleList.addTuple(tempTuple);
                }
            }
        }
        return tupleList;
    }

    public SelectResult getSelectResult(ArrayList<ClassTableItem> classTableItemList,TupleList tupleList){
        SelectResult selectResult=new SelectResult();
        selectResult.className=new String[classTableItemList.size()];
        selectResult.attrname=new String[classTableItemList.size()];
        selectResult.attrid=new int[classTableItemList.size()];
        selectResult.type=new String[classTableItemList.size()];
        for(int i=0;i<classTableItemList.size();i++){
            selectResult.className[i]=classTableItemList.get(i).classname;
            selectResult.attrid[i]=classTableItemList.get(i).attrid;
            selectResult.attrname[i]=classTableItemList.get(i).attrname;
            selectResult.type[i]=classTableItemList.get(i).attrtype;
        }
        selectResult.tpl=tupleList;
        return selectResult;
    }

    public HashMap<SelectItem,ArrayList<Column>> getSelectItemColumn(ArrayList<SelectItem> selectItemArrayList){
        HashMap<SelectItem,ArrayList<Column>> res=new HashMap<>();
        for(SelectItem selectItem:selectItemArrayList){
            ArrayList<Column> columns=new ArrayList<>();
            getSelectColumn((SimpleNode) selectItem.getASTNode(),columns);
            res.put(selectItem,columns);
        }
        return res;
    }

    public void getSelectColumn(SimpleNode node,ArrayList<Column> selectColumnList){
        int i=0;
        while(i<node.jjtGetNumChildren()){
            SimpleNode currentNode= (SimpleNode) node.jjtGetChild(i);
            String className= currentNode.toString();
            if(className.equals("Column")){
                selectColumnList.add((Column)currentNode.jjtGetValue());
            }
            if(currentNode.jjtGetNumChildren()>0) getSelectColumn((SimpleNode) currentNode,selectColumnList);
            i++;
        }
    }

    public ArrayList<Column> getSelectColumnList(HashMap<SelectItem,ArrayList<Column>> map){
        ArrayList<Column> res=new ArrayList<>();
        for(ArrayList<Column> columns:map.values()){
            res.addAll(columns);
        }
        return res;
    }

    public ArrayList<ClassTableItem> getJoinClassTableItem(ArrayList<ClassTableItem> list1, ArrayList<ClassTableItem> list2, Join join){
        LinkedList<Expression> expressionLinkedList=(LinkedList<Expression>)join.getOnExpressions();
        if(!expressionLinkedList.isEmpty()){
            EqualsTo equals=(EqualsTo) expressionLinkedList.get(0);
            Column rightExpression=(Column) equals.getRightExpression();
            for(ClassTableItem classTableItem:list2){
                if(classTableItem.attrname.equals(rightExpression.getColumnName())) continue;
                list1.add(classTableItem);
            }
        }
        else{
            for(ClassTableItem classTableItem:list2){
                list1.add(classTableItem);
            }
        }
        return list1;
    }
}
