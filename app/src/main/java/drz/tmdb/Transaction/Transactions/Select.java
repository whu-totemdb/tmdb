package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BinaryOperator;

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

    public SelectResult select(Object stmt){
        SelectBody selectBody = null;
        if(stmt.getClass().getSimpleName().equals("Select")) selectBody=((net.sf.jsqlparser.statement.select.Select)stmt).getSelectBody();
        else if(stmt.getClass().getSimpleName().equals("SubSelect")) selectBody=((SubSelect)stmt).getSelectBody();
        SelectResult res=new SelectResult();
        if((selectBody.getClass().getSimpleName().equals("SetOperationList"))){
            SetOperationList setOperationList= (SetOperationList) selectBody;
            return setOperation(setOperationList);
        }
        else if(selectBody.getClass().getSimpleName().equals("PlainSelect")){
            res=plainSelect(selectBody);
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
        if(plainSelect.getWhere()!=null){
            Where where=new Where();
            selectResult=where.where(plainSelect,selectResult);
        }
        selectResult=elicit(plainSelect,selectResult);
        return selectResult;
    }

    public SelectResult elicit(PlainSelect plainSelect,SelectResult selectResult){
        ArrayList<SelectItem> selectItemList= (ArrayList<SelectItem>) plainSelect.getSelectItems();
        int length=selectItemList.size();
        for(int i=0;i<selectItemList.size();i++) {
            if (selectItemList.get(i).getClass().getSimpleName().equals("AllColumns")) {
                for(int j=0;j<selectResult.attrid.length;j++){
                    selectResult.attrid[j]=j;
                }
                return selectResult;
            }
            else if(selectItemList.get(i).getClass().getSimpleName().equals("AllTableColumns")){
                AllTableColumns selectItem= (AllTableColumns) selectItemList.get(i);
                for(String s:selectResult.className){
                    if (s.equals(selectItem.getTable().getName()) ||
                            (selectItem.getTable().getAlias() != null && selectItem.getTable().getAlias().getName().equals(s))) {
                        length++;
                    }
                }
                length--;
            }
        }
        TupleList resTupleList=new TupleList();
        SelectResult result=new SelectResult();
        result.attrname=new String[length];
        result.attrid=new int[length];
        result.type=new String[length];
        for(int i=0;i<selectResult.tpl.tuplelist.size();i++){
            Tuple tuple=new Tuple();
            tuple.tuple=new Object[length];
            resTupleList.addTuple(tuple);
        }
        int i=0;
        int index=0;
        while(i<length){
            if(selectItemList.get(index).getClass().getSimpleName().equals("SelectExpressionItem")){
                SelectExpressionItem selectItem=(SelectExpressionItem) selectItemList.get(index);
                if(selectItem.getAlias()!=null){
                    result.attrname[i]=selectItem.getAlias().getName();
                }
                else result.attrname[i]=selectItem.toString();
                ArrayList<Object> thisColumn=(new Formula()).formulaExecute(selectItem.getExpression(),selectResult);
                for(int j=0;j<resTupleList.tuplelist.size();j++){
                    resTupleList.tuplelist.get(j).tuple[i]=thisColumn.get(j);
                }
                result.type[i]="char";
                result.attrid[i]=i;
                i++;
                index++;
            }
            else if(selectItemList.get(index).getClass().getSimpleName().equals("AllTableColumns")){
                AllTableColumns selectItem= (AllTableColumns) selectItemList.get(index);
                for(int j=0;j<selectResult.className.length;j++){
                    String s=selectResult.className[j];
                    if (s.equals(selectItem.getTable().getName()) ||
                            (selectItem.getTable().getAlias() != null && selectItem.getTable().getAlias().getName().equals(s))) {
                        result.attrname[i]=selectResult.attrname[j];
                        for(int x=0;x<resTupleList.tuplelist.size();x++){
                            resTupleList.tuplelist.get(x).tuple[i]=selectResult.tpl.tuplelist.get(x).tuple[j];
                        }
                        result.type[i]="char";
                        result.attrid[i]=i;
                        i++;
                    }
                }
                index++;
            }
        }
        result.tpl=resTupleList;
        return result;
    }

//    public ArrayList<Integer> getElicitIndexList(List<Column> selectColumnList,SelectResult selectResult){
//        ArrayList<Integer> elicitIndexList=new ArrayList<>();
//        for(Column column:selectColumnList){
//            for(int i=0;i<selectResult.attrname.length;i++){
//                if(selectResult.attrname[i].equals(column.getColumnName())) {
//                    if (column.getTable()==null || selectResult.className[i].equals(column.getTable().toString())) {
//                        elicitIndexList.add(i);
//                        break;
//                    }
//                }
//            }
//        }
//        return elicitIndexList;
//    }
//
//    public SelectResult getElicitSelectResult(ArrayList<Integer> elicitIndexList,SelectResult selectResult){
//        SelectResult res=new SelectResult();
//        res.className=new String[elicitIndexList.size()];
//        res.attrname=new String[elicitIndexList.size()];
//        res.attrid=new int[elicitIndexList.size()];
//        res.type=new String[elicitIndexList.size()];
//        for(int i=0;i<elicitIndexList.size();i++){
//            res.className[i]=selectResult.className[elicitIndexList.get(i)];
//            res.attrid[i]=selectResult.attrid[elicitIndexList.get(i)];
//            res.attrname[i]=selectResult.attrname[elicitIndexList.get(i)];
//            res.type[i]=selectResult.type[elicitIndexList.get(i)];
//        }
//        TupleList elicitTupleList=new TupleList();
//        for(Tuple tuple:selectResult.tpl.tuplelist) {
//            Tuple elicitTuple=new Tuple();
//            Object[] temp=new Object[elicitIndexList.size()];
//            for(int i=0;i<elicitIndexList.size();i++) {
//                temp[i]=tuple.tuple[elicitIndexList.get(i)];
//            }
//            elicitTuple.tuple=temp;
//            elicitTupleList.addTuple(elicitTuple);
//        }
//
//        res.tpl=elicitTupleList;
//        return res;
//    }

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
                classTableItemList.addAll(tempClassTableItem);
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
        TupleList tupleList=new TupleList();
        HashSet<Tuple> set=new HashSet<>();
        for(Tuple tuple:selectResult1.tpl.tuplelist){
            set.add(tuple);
        }
        for(Tuple tuple:selectResult2.tpl.tuplelist){
            set.add(tuple);
        }
        List<Tuple> res = new ArrayList<Tuple>(set);
        tupleList.tuplelist=res;
        tupleList.tuplenum=set.size();
        selectResult1.tpl=tupleList;
        return selectResult1;
    }

    public SelectResult intersect(SelectResult selectResult1,SelectResult selectResult2){
        TupleList tupleList=new TupleList();
        HashSet<Tuple> set=new HashSet<>();
        List<Tuple> res=new ArrayList<>();
        for(Tuple tuple:selectResult1.tpl.tuplelist){
            set.add(tuple);
        }
        for(Tuple tuple:selectResult2.tpl.tuplelist){
            if(set.contains(tuple)) res.add(tuple);
        }
        tupleList.tuplelist=res;
        tupleList.tuplenum=res.size();
        selectResult1.tpl=tupleList;
        return selectResult1;
    }

    public SelectResult except(SelectResult selectResult1,SelectResult selectResult2){
        TupleList tupleList=new TupleList();
        HashSet<Tuple> set=new HashSet<>();
        for(Tuple tuple:selectResult1.tpl.tuplelist){
            set.add(tuple);
        }
        for(Tuple tuple:selectResult2.tpl.tuplelist){
            if(set.contains(tuple)) set.remove(tuple);
        }
        List<Tuple> res = new ArrayList<Tuple>(set);
        tupleList.tuplelist=res;
        tupleList.tuplenum=set.size();
        selectResult1.tpl=tupleList;
        return selectResult1;
    }

    public SelectResult minus(SelectResult selectResult1,SelectResult selectResult2){
        TupleList tupleList=new TupleList();
        HashSet<Tuple> set=new HashSet<>();
        for(Tuple tuple:selectResult1.tpl.tuplelist){
            set.add(tuple);
        }
        for(Tuple tuple:selectResult2.tpl.tuplelist){
            if(set.contains(tuple)) set.remove(tuple);
        }
        List<Tuple> res = new ArrayList<Tuple>(set);
        tupleList.tuplelist=res;
        tupleList.tuplenum=set.size();
        selectResult1.tpl=tupleList;
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
        if(!expressionLinkedList.isEmpty()){
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
            else if(join.isLeft()){
                leftTupleList=leftJoin(leftTupleList,rightTupleList,leftIndex,rightIndex);
            }
            else if(join.isRight()){
                leftTupleList=rightJoin(leftTupleList,rightTupleList,leftIndex,rightIndex);
            }
            else if(join.isOuter()){
                leftTupleList=outerJoin(leftTupleList,rightTupleList,leftIndex,rightIndex);
            }
            else{
                leftTupleList=naturalJoin(leftTupleList,rightTupleList,leftIndex,rightIndex);
            }
        }
        return leftTupleList;
    }

    public TupleList naturalJoin(TupleList left,TupleList right,int leftIndex, int rightIndex){
        TupleList tupleList=new TupleList();
        for(Tuple leftTuple:left.tuplelist){
            for(Tuple rightTuple:right.tuplelist){
                if(leftTuple.tuple[leftIndex].equals(rightTuple.tuple[rightIndex])){
                    Tuple tempTuple=new Tuple();
                    int newLength = leftTuple.tuple.length + rightTuple.tuple.length;
                    Object[] tuple = new Object[newLength];
                    for (int i = 0; i < leftTuple.tuple.length; i++) {
                        tuple[i] = leftTuple.tuple[i];
                    }
                    for (int i = leftTuple.tuple.length; i < newLength; i++) {
                        tuple[i] = rightTuple.tuple[i-leftTuple.tuple.length];
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
        if(left.tuplelist.isEmpty()) return tupleList;
        HashMap<Object,ArrayList<Integer>> map=new HashMap<>();
        HashMap<Object, Boolean> check=new HashMap<>();
        for(int i=0;i<left.tuplelist.size();i++){
            Tuple leftT=left.tuplelist.get(i);
            if(map.containsKey(leftT.tuple[leftIndex])){
                map.get(leftT.tuple[leftIndex]).add(i);
            }
            else {
                ArrayList<Integer> list=new ArrayList<>();
                list.add(i);
                map.put(leftT.tuple[leftIndex], list);
                check.put(leftT.tuple[leftIndex],false);
            }
        }
        for(Tuple rightTuple:right.tuplelist){
            if(map.containsKey(rightTuple.tuple[rightIndex])){
                check.replace(rightTuple.tuple[rightIndex],true);
                for(int index:map.get(rightTuple.tuple[rightIndex])) {
                    Tuple leftTuple=left.tuplelist.get(index);
                    Tuple tempTuple = new Tuple();
                    int newLength = leftTuple.tuple.length + rightTuple.tuple.length;
                    Object[] tuple = new Object[newLength];
                    for (int i = 0; i < leftTuple.tuple.length; i++) {
                        tuple[i] = leftTuple.tuple[i];
                    }
                    for (int i = leftTuple.tuple.length; i < newLength; i++) {
                        tuple[i] = rightTuple.tuple[i-leftTuple.tuple.length];
                    }
                    tempTuple.tuple = tuple;
                    tupleList.addTuple(tempTuple);
                }
            }
        }
        for(Object obj:check.keySet()){
            if(check.get(obj)==false){
                for(int index:map.get(obj)){
                    int newLength = left.tuplelist.get(0).tuple.length + right.tuplelist.get(0).tuple.length;
                    Object[] tuple = new Object[newLength];
                    Tuple tempLeft=left.tuplelist.get(index);
                    for(int i=0;i<left.tuplelist.get(0).tuple.length;i++){
                        tuple[i]=tempLeft.tuple[i];
                    }
                    Tuple tempTuple = new Tuple();
                    tempTuple.tuple=tuple;
                    tupleList.addTuple(tempTuple);
                }
            }
        }

        return tupleList;
    }

    public TupleList rightJoin(TupleList left,TupleList right,int leftIndex, int rightIndex){
        TupleList tupleList=new TupleList();
        if(right.tuplelist.isEmpty()) return tupleList;
        HashMap<Object,ArrayList<Integer>> map=new HashMap<>();
        HashMap<Object, Boolean> check=new HashMap<>();
        for(int i=0;i<right.tuplelist.size();i++){
            Tuple rightT=right.tuplelist.get(i);
            if(map.containsKey(rightT.tuple[rightIndex])){
                map.get(rightT.tuple[rightIndex]).add(i);
            }
            else {
                ArrayList<Integer> list=new ArrayList<>();
                list.add(i);
                map.put(rightT.tuple[rightIndex], list);
                check.put(rightT.tuple[rightIndex],false);
            }
        }
        for(Tuple leftTuple:left.tuplelist){
            if(map.containsKey(leftTuple.tuple[leftIndex])){
                check.replace(leftTuple.tuple[leftIndex],true);
                for(int index:map.get(leftTuple.tuple[leftIndex])) {
                    Tuple rightTuple=right.tuplelist.get(index);
                    Tuple tempTuple = new Tuple();
                    int newLength = leftTuple.tuple.length + rightTuple.tuple.length;
                    Object[] tuple = new Object[newLength];
                    for (int i = 0; i < leftTuple.tuple.length; i++) {
                        tuple[i] = leftTuple.tuple[i];
                    }
                    for (int i = leftTuple.tuple.length; i < newLength; i++) {
                        tuple[i] = rightTuple.tuple[i-leftTuple.tuple.length];
                    }
                    tempTuple.tuple = tuple;
                    tupleList.addTuple(tempTuple);
                }
            }
        }
        for(Object obj:check.keySet()){
            if(check.get(obj)==false){
                for(int index:map.get(obj)){
                    int newLength = left.tuplelist.get(0).tuple.length + right.tuplelist.get(0).tuple.length;
                    Object[] tuple = new Object[newLength];
                    Tuple tempRight=right.tuplelist.get(index);
                    for(int i=left.tuplelist.get(0).tuple.length;i<newLength;i++){
                        tuple[i]=tempRight.tuple[i-left.tuplelist.get(0).tuple.length];
                    }
                    Tuple tempTuple = new Tuple();
                    tempTuple.tuple=tuple;
                    tupleList.addTuple(tempTuple);
                }
            }
        }

        return tupleList;
    }

    public TupleList outerJoin(TupleList left,TupleList right,int leftIndex, int rightIndex){
        TupleList tupleList=new TupleList();
        if(left.tuplelist.isEmpty()) return tupleList;
        HashMap<Object,ArrayList<Integer>> mapLeft=new HashMap<>();
        HashMap<Object,ArrayList<Integer>> mapRight=new HashMap<>();
        HashMap<Object, Boolean> checkLeft=new HashMap<>();
        HashMap<Object, Boolean> checkRight=new HashMap<>();
        for(int i=0;i<left.tuplelist.size();i++){
            Tuple leftT=left.tuplelist.get(i);
            if(mapLeft.containsKey(leftT.tuple[leftIndex])){
                mapLeft.get(leftT.tuple[leftIndex]).add(i);
            }
            else {
                ArrayList<Integer> list=new ArrayList<>();
                list.add(i);
                mapLeft.put(leftT.tuple[leftIndex], list);
                checkLeft.put(leftT.tuple[leftIndex],false);
            }
        }
        for(int i=0;i<right.tuplelist.size();i++){
            Tuple rightT=right.tuplelist.get(i);
            if(mapRight.containsKey(rightT.tuple[rightIndex])){
                mapRight.get(rightT.tuple[rightIndex]).add(i);
            }
            else {
                ArrayList<Integer> list=new ArrayList<>();
                list.add(i);
                mapRight.put(rightT.tuple[rightIndex], list);
                checkRight.put(rightT.tuple[rightIndex],false);
            }
        }
        for(Tuple rightTuple:right.tuplelist){
            if(mapLeft.containsKey(rightTuple.tuple[rightIndex])){
                checkLeft.replace(rightTuple.tuple[rightIndex],true);
                checkRight.replace(rightTuple.tuple[rightIndex],true);
                for(int index:mapLeft.get(rightTuple.tuple[rightIndex])) {
                    Tuple leftTuple=left.tuplelist.get(index);
                    Tuple tempTuple = new Tuple();
                    int newLength = leftTuple.tuple.length + rightTuple.tuple.length;
                    Object[] tuple = new Object[newLength];
                    for (int i = 0; i < leftTuple.tuple.length; i++) {
                        tuple[i] = leftTuple.tuple[i];
                    }
                    for (int i = leftTuple.tuple.length; i < newLength; i++) {
                        tuple[i] = rightTuple.tuple[i-leftTuple.tuple.length];
                    }
                    tempTuple.tuple = tuple;
                    tupleList.addTuple(tempTuple);
                }
            }
        }
        for(Object obj:checkLeft.keySet()){
            if(checkLeft.get(obj)==false){
                for(int index:mapLeft.get(obj)){
                    int newLength = left.tuplelist.get(0).tuple.length + right.tuplelist.get(0).tuple.length;
                    Object[] tuple = new Object[newLength];
                    Tuple tempLeft=left.tuplelist.get(index);
                    for(int i=0;i<left.tuplelist.get(0).tuple.length;i++){
                        tuple[i]=tempLeft.tuple[i];
                    }
                    Tuple tempTuple = new Tuple();
                    tempTuple.tuple=tuple;
                    tupleList.addTuple(tempTuple);
                }
            }
        }
        for(Object obj:checkRight.keySet()){
            if(checkRight.get(obj)==false){
                for(int index:mapRight.get(obj)){
                    int newLength = left.tuplelist.get(0).tuple.length + right.tuplelist.get(0).tuple.length;
                    Object[] tuple = new Object[newLength];
                    Tuple tempRight=right.tuplelist.get(index);
                    for(int i=left.tuplelist.get(0).tuple.length;i<newLength;i++){
                        tuple[i]=tempRight.tuple[i-left.tuplelist.get(0).tuple.length];
                    }
                    Tuple tempTuple = new Tuple();
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

}
