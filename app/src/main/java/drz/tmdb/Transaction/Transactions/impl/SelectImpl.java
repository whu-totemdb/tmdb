package drz.tmdb.Transaction.Transactions.impl;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import drz.tmdb.memory.SystemTable.ObjectTableItem;
import drz.tmdb.memory.Tuple;
import drz.tmdb.memory.TupleList;
import drz.tmdb.memory.SystemTable.ClassTableItem;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.utils.Formula;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;
import drz.tmdb.Transaction.Transactions.utils.SelectResult;

//1、from子句组装来自不同数据源的数据；
//2、where子句基于指定的条件对记录行进行筛选；
//3、group by子句将数据划分为多个分组；
//4、使用聚集函数进行计算；
//5、使用having子句筛选分组；
//6、计算所有的表达式；
//7、select 的字段；
//8、使用order by对结果集进行排序。
public class SelectImpl implements drz.tmdb.Transaction.Transactions.Select {

    private MemConnect memConnect;

    public SelectImpl(MemConnect memConnect) {
        this.memConnect = memConnect;
    }

    public SelectImpl() {
    }

    @Override
    public SelectResult select(Object stmt) throws TMDBException {
        SelectBody selectBody = null;
        //如果语法树的形式是Select，将查询主题赋值给selectBody
        if(stmt.getClass().getSimpleName().equals("Select")) selectBody=((net.sf.jsqlparser.statement.select.Select)stmt).getSelectBody();
        //如果语法树形式是subselect（subSelect也要走select的逻辑）
        else if(stmt.getClass().getSimpleName().equals("SubSelect")) selectBody=((SubSelect)stmt).getSelectBody();

        SelectResult res=new SelectResult();

        //如果selectBody形式是SetOperationList，那种带union，except的查询就是这种
        if((selectBody.getClass().getSimpleName().equals("SetOperationList"))){
            SetOperationList setOperationList= (SetOperationList) selectBody;
            return setOperation(setOperationList);
        }
        //如果selectBody只是一个plainSelct
        else if(selectBody.getClass().getSimpleName().equals("PlainSelect")){
            res=plainSelect(selectBody);
        }
        return res;
    }

    public SelectResult plainSelect(SelectBody stmt) throws TMDBException {
//      //Values 也是一种plainSelect，如果是values，由values方法处理
        if(stmt.getClass().getSimpleName().equals("ValuesStatement")) return values((ValuesStatement) stmt);

        PlainSelect plainSelect= (PlainSelect) stmt;

        //如果是对象代理的跨类查询，则用
        if(plainSelect.isDeputySelect()==true){
            return deputySelect((PlainSelect) stmt);
        }

        //以下是正常的plainselect逻辑，from->where->select
        //先是from从存储中拿到数据
        SelectResult selectResult=from(plainSelect);
        //通过where进行筛选
        if(plainSelect.getWhere()!=null){
            Where where=new Where(memConnect);
            selectResult=where.where(plainSelect,selectResult);
        }
        //然后通过selectItem提取想要的列
        selectResult=elicit(plainSelect,selectResult);
        //最终返回selectResult
        return selectResult;
    }

    //提取
    public SelectResult elicit(PlainSelect plainSelect,SelectResult selectResult) throws TMDBException {
        ArrayList<SelectItem> selectItemList= (ArrayList<SelectItem>) plainSelect.getSelectItems();
        HashMap<SelectItem, ArrayList<Column>> map = getSelectItemColumn(selectItemList);

        int length=selectItemList.size();
        for(int i=0;i<selectItemList.size();i++) {
            //如果当前情况是select * from，则显示作为选择allcolumns，直接返回全部
            if (selectItemList.get(i).getClass().getSimpleName().equals("AllColumns")) {
                selectResult.setAlias(selectResult.getAttrname());
                //要对attrid重拍以下，不然最终printresult的时候会有问题
                for(int j=0;j<selectResult.getAttrid().length;j++){
                    selectResult.getAttrid()[j]=j;
                }
                return selectResult;
            }
            //如果是select a.* from a，b这种，a.*会显示为AllTableColumns，这时候，需要将a的所有的元素都加入结果集合中
            //也就是说a.*虽然在selectItem中只显示为一个元素，但是需要输出多个元素，因此要更改以下length
            else if(selectItemList.get(i).getClass().getSimpleName().equals("AllTableColumns")){
                AllTableColumns selectItem= (AllTableColumns) selectItemList.get(i);
                for(int y=0;y<selectResult.getClassName().length;y++){
                    String s=selectResult.getClassName()[y];
                    String alias=selectResult.getAlias()[y];
                    if (s.equals(selectItem.getTable().getName()) || selectItem.getTable().getAlias()!=null && selectItem.getTable().getAlias().getName().equals(alias)) {
                        length++;
                    }
                }
                length--;
            }
        }
        TupleList resTupleList=new TupleList();
        SelectResult result=new SelectResult();
        result.setAlias(new String[length]);
        result.setAttrname(new String[length]);
        result.setClassName(new String[length]);
        result.setAttrid(new int[length]);
        result.setType(new String[length]);
        for(int i=0;i<selectResult.getTpl().tuplelist.size();i++){
            Tuple tuple=new Tuple();
            int[] temp=new int[length];
            Arrays.fill(temp,-1);
            tuple.tupleIds=temp;

            tuple.tuple=new Object[length];
            resTupleList.addTuple(tuple);
        }
        int i=0;
        int index=0;
        while(i<length){
            //遍历selectItemList中的selectItem，分别处理
            //这里是针对selectItem是表达式的判定，a或者a+2或者a+b*c都会被认定为表达式
            if(selectItemList.get(index).getClass().getSimpleName().equals("SelectExpressionItem")){
                SelectExpressionItem selectItem=(SelectExpressionItem) selectItemList.get(index);
                //如果有alias 例如a*b as c则需要将输出的getAttrname()改成别名
                result.getAlias()[i]=selectResult.getAttrname()[index];
                if(selectItem.getAlias()!=null){
                    result.getAttrname()[i]=selectItem.getAlias().getName();
                }
                else result.getAttrname()[i]=selectItem.toString();
                //调用formula对表达式进行解析，返回运算后的结果，存入res的tuplelist中
                ArrayList<Object> thisColumn=(new Formula()).formulaExecute(selectItem.getExpression(),selectResult);
                int tempI=-1;
                Column column = map.get(selectItem).get(0);
                for (int j = 0; j < selectResult.getClassName().length; j++) {
                    if(column.getTable()!=null){
                        if((selectResult.getClassName()[j].equals(column.getTable().getName())
                        || selectResult.getAlias()[j].equals(column.getTable().getName()))
                        && selectResult.getAttrname()[j].equals(column.getColumnName())){
                            tempI=j;
                            break;
                        }
                    }
                    else{
                        if(selectResult.getAttrname()[j].equals(column.getColumnName())){
                            tempI=j;
                            break;
                        }
                    }
                }

                for(int j=0;j<resTupleList.tuplelist.size();j++){
                    resTupleList.tuplelist.get(j).tuple[i]=thisColumn.get(j);
                    resTupleList.tuplelist.get(j).tupleIds[i]=selectResult.getTpl().tuplelist.get(j).tupleIds[tempI];
                }

                result.getType()[i]=selectResult.getType()[tempI];
                result.getAttrid()[i]=i;
                result.getClassName()[i]=selectResult.getClassName()[tempI];
                i++;
                index++;
            }
            //这里是针对a.*这种情况的操作
            else if(selectItemList.get(index).getClass().getSimpleName().equals("AllTableColumns")){
                AllTableColumns selectItem= (AllTableColumns) selectItemList.get(index);
                for(int j=0;j<selectResult.getClassName().length;j++){
                    String s=selectResult.getClassName()[j];
                    String alias=selectResult.getAlias()[j];
                    //将选取表的所有列加入结果集合中
                    if (s.equals(selectItem.getTable().getName()) || selectItem.getTable().getName().equals(alias)) {
                        result.getAttrname()[i]=selectResult.getAttrname()[j];
                        result.getAlias()[i]=selectResult.getAttrname()[j];
                        for(int x=0;x<resTupleList.tuplelist.size();x++){
                            resTupleList.tuplelist.get(x).tuple[i]=selectResult.getTpl().tuplelist.get(x).tuple[j];
                            resTupleList.tuplelist.get(x).tupleIds[i]=selectResult.getTpl().tuplelist.get(x).tupleIds[j];
                        }
                        result.getType()[i]=selectResult.getType()[index];
                        result.getAttrid()[i]=i;
                        i++;
                    }
                }
                index++;
            }
        }
        result.setTpl(resTupleList);
        return result;
    }

//    public ArrayList<Integer> getElicitIndexList(List<Column> selectColumnList,SelectResult selectResult){
//        ArrayList<Integer> elicitIndexList=new ArrayList<>();
//        for(Column column:selectColumnList){
//            for(int i=0;i<selectResult.getAttrname().length;i++){
//                if(selectResult.getAttrname()[i].equals(column.getColumnName())) {
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
//        res.getAttrname()=new String[elicitIndexList.size()];
//        res.attrid=new int[elicitIndexList.size()];
//        res.type=new String[elicitIndexList.size()];
//        for(int i=0;i<elicitIndexList.size();i++){
//            res.className[i]=selectResult.className[elicitIndexList.get(i)];
//            res.attrid[i]=selectResult.attrid[elicitIndexList.get(i)];
//            res.getAttrname()[i]=selectResult.getAttrname()[elicitIndexList.get(i)];
//            res.type[i]=selectResult.type[elicitIndexList.get(i)];
//        }
//        TupleList elicitTupleList=new TupleList();
//        for(Tuple tuple:selectResult.getTpl().tuplelist) {
//            Tuple elicitTuple=new Tuple();
//            Object[] temp=new Object[elicitIndexList.size()];
//            for(int i=0;i<elicitIndexList.size();i++) {
//                temp[i]=tuple.tuple[elicitIndexList.get(i)];
//            }
//            elicitTuple.tuple=temp;
//            elicitTupleList.addTuple(elicitTuple);
//        }
//
//        res.getTpl()=elicitTupleList;
//        return res;
//    }

    //from部分
    public SelectResult from(PlainSelect plainSelect) throws TMDBException {
        //获取plainselect的fromItem（多表查询的话，会取第一个table名）
        FromItem fromItem=plainSelect.getFromItem();
        //获取这个table对应的tuple
        TupleList tupleList=getTable(fromItem);
        //这是这个table对应的table Item
        ArrayList<ClassTableItem> classTableItemList=this.getSelectItem(fromItem);
        //通过classTableItemList和tuplelist获取selectResult（基本所有操作都针对selectresult进行）
        SelectResult selectResult=getSelectResult(classTableItemList,tupleList);
        //进行join操作
        if(!(plainSelect.getJoins() ==null)){
            for(Join join:plainSelect.getJoins()){
                //获取当前join表的的一些元祖
                ArrayList<ClassTableItem> tempClassTableItem=this.getSelectItem(join.getRightItem());
                TupleList tempTupleList=getTable(join.getRightItem());
                SelectResult tempSelectResult=getSelectResult(tempClassTableItem,tempTupleList);

                //将本来的TupleList和当前操作的join的tuplelist根据join的形式进行组合
                tupleList=join(selectResult,tempSelectResult,join);
                //把classTableItem进行合并
                classTableItemList.addAll(tempClassTableItem);
                //根据join后的tuple和合并后的classtableItemlist生成selectResult
                selectResult=getSelectResult(classTableItemList,tupleList);
            }
        }
        return selectResult;
    }

    //跨类查询。。。。
    public SelectResult deputySelect(PlainSelect plainSelect) throws TMDBException {
        ArrayList<SelectItem> selectItemList=(ArrayList<SelectItem>)plainSelect.getSelectItems();
        FromItem fromItem=plainSelect.getFromItem();
        TupleList tupleList=getTable(fromItem);
        HashMap<SelectItem,ArrayList<Column>> selectItemToColumn=getSelectItemColumn(selectItemList);
        List<Column> selectColumnList=getSelectColumnList(selectItemToColumn);
        ArrayList<ClassTableItem> classTableItemList=this.getSelectItem(fromItem,selectColumnList);
        return getSelectResult(classTableItemList,tupleList);
    }

    //进行union这种操作的方法
    public SelectResult setOperation(SetOperationList setOperationList) throws TMDBException {
        SelectResult selectResult=new SelectResult();
        //提取出不同的select
        List<SelectBody> plainSelectList=setOperationList.getSelects();
        //提取出不同的操作符
        List<SetOperation> setOperationList1=setOperationList.getOperations();
        //首先获得第一个plainselect的selectResult，之后在这个之上进行累加或者删减
        selectResult=plainSelect(plainSelectList.get(0));
        //对后面的plainSelect遍历获取selectResult，然后根据setOperation的规则进行操作
        for(int i=1;i<plainSelectList.size();i++){
            //拿到第i个plainselect的selectResult
            SelectResult tempResult=plainSelect((PlainSelect) plainSelectList.get(i));
            //根据当前setOperation的规则进行操作。
            selectResult=Operate(selectResult,tempResult,setOperationList1.get(i-1));
        }
        return selectResult;
    }

    //setOperation核心方法
    public SelectResult Operate(SelectResult selectResult1, SelectResult selectResult2, SetOperation setOperation) throws TMDBException {
        if(selectResult1.getAttrname().length!=selectResult2.getAttrname().length) throw new TMDBException("无法进行集合操作");
        //根据setOperation的种类进行操作划分
        switch (setOperation.toString()){
            case "UNION": return union(selectResult1,selectResult2);
            case "INTERSECT": return intersect(selectResult1,selectResult2);
            case "EXCEPT": return except(selectResult1,selectResult2);
            case "MINUS": return minus(selectResult1,selectResult2);
        }
        return null;
    }

    //union操作
    public SelectResult union(SelectResult selectResult1,SelectResult selectResult2){
        TupleList tupleList=new TupleList();
        HashSet<Tuple> set=new HashSet<>();
        //将两个tuplelist都加入最终的结果集合里
        for(Tuple tuple:selectResult1.getTpl().tuplelist){
            set.add(tuple);
        }
        for(Tuple tuple:selectResult2.getTpl().tuplelist){
            set.add(tuple);
        }
        List<Tuple> res = new ArrayList<Tuple>(set);
        tupleList.tuplelist=res;
        tupleList.tuplenum=set.size();
        selectResult1.setTpl(tupleList);
        return selectResult1;
    }

    //intersect操作
    public SelectResult intersect(SelectResult selectResult1,SelectResult selectResult2){
        TupleList tupleList=new TupleList();
        HashSet<Tuple> set=new HashSet<>();
        List<Tuple> res=new ArrayList<>();
        for(Tuple tuple:selectResult1.getTpl().tuplelist){
            set.add(tuple);
        }
        //如果2中含有1中的tuple，则加入结果集合中
        for(Tuple tuple:selectResult2.getTpl().tuplelist){
            if(set.contains(tuple)) res.add(tuple);
        }
        tupleList.tuplelist=res;
        tupleList.tuplenum=res.size();
        selectResult1.setTpl(tupleList);
        return selectResult1;
    }

    public SelectResult except(SelectResult selectResult1,SelectResult selectResult2){
        TupleList tupleList=new TupleList();
        HashSet<Tuple> set=new HashSet<>();
        for(Tuple tuple:selectResult1.getTpl().tuplelist){
            set.add(tuple);
        }
        //如果2中含有tuple，则在结果集合中移除
        for(Tuple tuple:selectResult2.getTpl().tuplelist){
            if(set.contains(tuple)) set.remove(tuple);
        }
        List<Tuple> res = new ArrayList<Tuple>(set);
        tupleList.tuplelist=res;
        tupleList.tuplenum=set.size();
        selectResult1.setTpl(tupleList);
        return selectResult1;
    }

    //和except逻辑一样
    public SelectResult minus(SelectResult selectResult1,SelectResult selectResult2){
        TupleList tupleList=new TupleList();
        HashSet<Tuple> set=new HashSet<>();
        for(Tuple tuple:selectResult1.getTpl().tuplelist){
            set.add(tuple);
        }
        for(Tuple tuple:selectResult2.getTpl().tuplelist){
            if(set.contains(tuple)) set.remove(tuple);
        }
        List<Tuple> res = new ArrayList<Tuple>(set);
        tupleList.tuplelist=res;
        tupleList.tuplenum=set.size();
        selectResult1.setTpl(tupleList);
        return selectResult1;
    }

    //针对values的处理
    public SelectResult values(ValuesStatement valuesStatement){
        ExpressionList expressionList= (ExpressionList) valuesStatement.getExpressions();
        List<Expression> expressions=expressionList.getExpressions();
        TupleList tupleList=new TupleList();
        for(int i=0;i<expressions.size();i++){
            if(expressions.get(i).getClass().getSimpleName().equals("RowConstructor")) {
                //values按照行进行存储
                RowConstructor rowConstructor = (RowConstructor) expressions.get(i);
                ExpressionList expressionList1 = rowConstructor.getExprList();
                Object[] tuple = new Object[expressionList1.getExpressions().size()];
                //将每行的值传到新建的tuple中
                for (int j = 0; j < expressionList1.getExpressions().size(); j++) {
                    tuple[j] = expressionList1.getExpressions().get(j).toString();
                }
                tupleList.addTuple(new Tuple(tuple));
            }
            else if(expressions.get(i).getClass().getSimpleName().equals("Parenthesis")){
                Parenthesis parenthesis = (Parenthesis) expressions.get(i);
                Expression expression = parenthesis.getExpression();
                Object[] tuple = new Object[]{expression.toString()};
                tupleList.addTuple(new Tuple(tuple));
            }
        }
        ArrayList<ClassTableItem> classTableItemArrayList=new ArrayList<>();
        //构建classtableItemlist，返回selctResult需要
        for(int i=0;i<tupleList.tuplelist.get(0).tuple.length;i++){
            ClassTableItem classTableItem=new ClassTableItem("", -1, tupleList.tuplelist.get(0).tuple.length,i, "attr"+i,tupleList.tuplelist.get(0).tuple.getClass().getSimpleName(),"","");
            classTableItemArrayList.add(classTableItem);
        }
        return getSelectResult(classTableItemArrayList,tupleList);
    }

    //join的核心方法
    public TupleList join(SelectResult left,SelectResult right,Join join) throws TMDBException {
        //左边的tuplelist
        TupleList leftTupleList=left.getTpl();
        //右边的tuplelist
        TupleList rightTupleList=right.getTpl();
        //获取onExpression的表达式list（可以有多个on，但是这里只实现了一个的）
        LinkedList<Expression> expressionLinkedList=(LinkedList<Expression>)join.getOnExpressions();
        if(!expressionLinkedList.isEmpty()){
            //默认只有一个onExpression 且为等于
            EqualsTo equals=(EqualsTo) expressionLinkedList.get(0);
            //获取等于表达式的左边
            Column leftExpression=(Column) equals.getLeftExpression();
            //获取等于表达式的右边
            Column rightExpression=(Column) equals.getRightExpression();
            //获取等于表达式的左表达式和右表达式在分别selectresult中的index。例如test.a=company.b 获取a和b在各自表的index
            int leftIndex=-1;
            int rightIndex=-1;
            for(int i=0;i<left.getAttrname().length;i++){
                if(leftExpression.getColumnName().equals(left.getAttrname()[i])){
                    leftIndex=i;
                    break;
                }
            }
            if(leftIndex==-1) throw new TMDBException("找不到"+leftExpression.getColumnName());
            for(int i=0;i<right.getAttrname().length;i++){
                if(rightExpression.getColumnName().equals(right.getAttrname()[i])){
                    rightIndex=i;
                    break;
                }
            }
            if(rightIndex==-1) throw new TMDBException("找不到"+rightExpression.getColumnName());
            //innerJoin
            if(join.isNatural() || join.isInner()){
                leftTupleList=naturalJoin(leftTupleList,rightTupleList,leftIndex,rightIndex);
            }
            //leftJoin
            else if(join.isLeft()){
                leftTupleList=leftJoin(leftTupleList,rightTupleList,leftIndex,rightIndex);
            }
            //rightJoin
            else if(join.isRight()){
                leftTupleList=rightJoin(leftTupleList,rightTupleList,leftIndex,rightIndex);
            }
            //outerJoin
            else if(join.isOuter()){
                leftTupleList=outerJoin(leftTupleList,rightTupleList,leftIndex,rightIndex);
            }
            //naturalJoin
            else if(join.isNatural()){
                leftTupleList=naturalJoin(leftTupleList,rightTupleList,leftIndex,rightIndex);
            }
        }
        else{
            //如果没有join的话，直接进行拼接
            TupleList tupleList = new TupleList();
            for(Tuple tuple:rightTupleList.tuplelist){
                for (Tuple t : leftTupleList.tuplelist) {
                    Tuple newTuple=new Tuple(Stream.concat(Arrays.stream(t.tuple),Arrays.stream(tuple.tuple)).toArray());
                    newTuple.tupleIds= IntStream.concat(Arrays.stream(t.tupleIds),Arrays.stream(tuple.tupleIds)).toArray();
                    newTuple.setTupleId(t.getTupleId());
                    tupleList.addTuple(newTuple);
                }
            }
            return tupleList;
        }
        return leftTupleList;
    }

    public TupleList naturalJoin(TupleList left,TupleList right,int leftIndex, int rightIndex){
        TupleList tupleList=new TupleList();
        //进行naturalJoin，判断在相连元素是否相等，等于才加入结果集中
        for(Tuple leftTuple:left.tuplelist){
            for(Tuple rightTuple:right.tuplelist){
                if(leftTuple.tuple[leftIndex].equals(rightTuple.tuple[rightIndex])){
                    Tuple tempTuple=new Tuple();
                    int newLength = leftTuple.tuple.length + rightTuple.tuple.length;
                    Object[] tuple = new Object[newLength];
                    int[] ids=new int[newLength];
                    for (int i = 0; i < leftTuple.tuple.length; i++) {
                        tuple[i] = leftTuple.tuple[i];
                        ids[i]=leftTuple.tupleIds[i];
                    }
                    for (int i = leftTuple.tuple.length; i < newLength; i++) {
                        tuple[i] = rightTuple.tuple[i-leftTuple.tuple.length];
                        ids[i]=rightTuple.tupleIds[i-leftTuple.tuple.length];
                    }
                    tempTuple.setTupleId(leftTuple.getTupleId());
                    tempTuple.tuple=tuple;
                    tempTuple.tupleIds=ids;
                    tupleList.addTuple(tempTuple);
                }
            }
        }
        return tupleList;
    }

    public TupleList leftJoin(TupleList left,TupleList right,int leftIndex, int rightIndex){
        //leftjoin，如果右边对上了，就加上右边元祖，如果没对上，就加入空元祖。
        TupleList tupleList=new TupleList();
        if(left.tuplelist.isEmpty()) return tupleList;
        HashMap<Object,ArrayList<Integer>> map=new HashMap<>();
        HashMap<Object, Boolean> check=new HashMap<>();
        //使用map存储左边的元素在连接处的元素值
        //使用check存储当前元素在右边是否有被使用
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
        //先遍历右边的tuplelist，然后对
        for(Tuple rightTuple:right.tuplelist){
            if(map.containsKey(rightTuple.tuple[rightIndex])){
                //如果右边有这个元素，修改check为true
                check.replace(rightTuple.tuple[rightIndex],true);
                //在res中加入每个左边的tuple拼接上当前匹配上的右边的tuple。
                // 例如左边有2个join处匹配上了当前右边的tuple，那么这两个都要拼接上右边的tuple，然后加入结果集合
                for(int index:map.get(rightTuple.tuple[rightIndex])) {
                    Tuple leftTuple=left.tuplelist.get(index);
                    Tuple tempTuple = new Tuple();
                    tempTuple.setTupleId(leftTuple.getTupleId());
                    int newLength = leftTuple.tuple.length + rightTuple.tuple.length;
                    Object[] tuple = new Object[newLength];
                    int[] ids = new int[newLength];

                    for (int i = 0; i < leftTuple.tuple.length; i++) {
                        tuple[i] = leftTuple.tuple[i];
                        ids[i] = leftTuple.tupleIds[i];
                    }
                    for (int i = leftTuple.tuple.length; i < newLength; i++) {
                        tuple[i] = rightTuple.tuple[i-leftTuple.tuple.length];
                        ids[i] = rightTuple.tupleIds[i-leftTuple.tuple.length];
                    }
                    tempTuple.tuple = tuple;
                    tempTuple.tupleIds=ids;
                    tupleList.addTuple(tempTuple);
                }
            }
        }
        //针对没有被匹配上的左边的tuple，拼接空元祖加入结果集合
        for(Object obj:check.keySet()){
            if(check.get(obj)==false){
                for(int index:map.get(obj)){
                    int newLength = left.tuplelist.get(0).tuple.length + right.tuplelist.get(0).tuple.length;
                    Object[] tuple = new Object[newLength];
                    int[] ids = new int[newLength];
                    Arrays.fill(ids,-1);
                    Tuple tempLeft=left.tuplelist.get(index);
                    for(int i=0;i<left.tuplelist.get(0).tuple.length;i++){
                        tuple[i]=tempLeft.tuple[i];
                        ids[i]=tempLeft.tupleIds[i];
                    }
                    Tuple tempTuple = new Tuple();
                    tempTuple.tuple=tuple;
                    tempTuple.tupleIds=ids;
                    tempTuple.setTupleId(tempLeft.getTupleId());
                    tupleList.addTuple(tempTuple);
                }
            }
        }

        return tupleList;
    }

    //逻辑和leftjoin类似
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
                    tempTuple.setTupleId(rightTuple.getTupleId());
                    int newLength = leftTuple.tuple.length + rightTuple.tuple.length;
                    Object[] tuple = new Object[newLength];
                    int[] ids = new int[newLength];
                    for (int i = 0; i < leftTuple.tuple.length; i++) {
                        tuple[i] = leftTuple.tuple[i];
                        ids[i] = leftTuple.tupleIds[i];
                    }
                    for (int i = leftTuple.tuple.length; i < newLength; i++) {
                        tuple[i] = rightTuple.tuple[i-leftTuple.tuple.length];
                        ids[i] = rightTuple.tupleIds[i-leftTuple.tuple.length];
                    }
                    tempTuple.tuple = tuple;
                    tempTuple.tupleIds=ids;
                    tupleList.addTuple(tempTuple);
                }
            }
        }
        for(Object obj:check.keySet()){
            if(check.get(obj)==false){
                for(int index:map.get(obj)){
                    int newLength = left.tuplelist.get(0).tuple.length + right.tuplelist.get(0).tuple.length;
                    Object[] tuple = new Object[newLength];
                    int[] ids = new int[newLength];
                    Arrays.fill(ids,-1);
                    Tuple tempRight=right.tuplelist.get(index);
                    for(int i=left.tuplelist.get(0).tuple.length;i<newLength;i++){
                        tuple[i]=tempRight.tuple[i-left.tuplelist.get(0).tuple.length];
                        ids[i]=tempRight.tupleIds[i-left.tuplelist.get(0).tuple.length];
                    }
                    Tuple tempTuple = new Tuple();
                    tempTuple.setTupleId(tempRight.getTupleId());
                    tempTuple.tuple=tuple;
                    tempTuple.tupleIds=ids;
                    tupleList.addTuple(tempTuple);
                }
            }
        }

        return tupleList;
    }

    //就是一个leftjoin和rightjoin的综合版
    public TupleList outerJoin(TupleList left,TupleList right,int leftIndex, int rightIndex){
        TupleList tupleList=new TupleList();
        if(left.tuplelist.isEmpty()) return tupleList;
        HashMap<Object,ArrayList<Integer>> mapLeft=new HashMap<>();
        HashMap<Object,ArrayList<Integer>> mapRight=new HashMap<>();
        HashMap<Object, Boolean> checkLeft=new HashMap<>();
        HashMap<Object, Boolean> checkRight=new HashMap<>();
        //分别用mapLeft存储左数组的join处的值对应的tupleindex和mapRight存右边的
        //然后checkLeft存左边数据的访问情况，checkRight存右边的
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
                    int[] ids = new int[newLength];
                    for (int i = 0; i < leftTuple.tuple.length; i++) {
                        tuple[i] = leftTuple.tuple[i];
                        ids[i] = leftTuple.tupleIds[i];
                    }
                    for (int i = leftTuple.tuple.length; i < newLength; i++) {
                        tuple[i] = rightTuple.tuple[i-leftTuple.tuple.length];
                        ids[i] = rightTuple.tupleIds[i-leftTuple.tuple.length];
                    }
                    tempTuple.tuple = tuple;
                    tempTuple.tupleIds = ids;
                    tupleList.addTuple(tempTuple);
                }
            }
        }
        for(Object obj:checkLeft.keySet()){
            if(checkLeft.get(obj)==false){
                for(int index:mapLeft.get(obj)){
                    int newLength = left.tuplelist.get(0).tuple.length + right.tuplelist.get(0).tuple.length;
                    Object[] tuple = new Object[newLength];
                    int[] ids = new int[newLength];
                    Arrays.fill(ids,-1);
                    Tuple tempLeft=left.tuplelist.get(index);
                    for(int i=0;i<left.tuplelist.get(0).tuple.length;i++){
                        tuple[i]=tempLeft.tuple[i];
                        ids[i]=tempLeft.tupleIds[i];
                    }
                    Tuple tempTuple = new Tuple();
                    tempTuple.tuple=tuple;
                    tempTuple.tupleIds=ids;
                    tupleList.addTuple(tempTuple);
                }
            }
        }
        for(Object obj:checkRight.keySet()){
            if(checkRight.get(obj)==false){
                for(int index:mapRight.get(obj)){
                    int newLength = left.tuplelist.get(0).tuple.length + right.tuplelist.get(0).tuple.length;
                    Object[] tuple = new Object[newLength];
                    int[] ids = new int[newLength];
                    Arrays.fill(ids,-1);
                    Tuple tempRight=right.tuplelist.get(index);
                    for(int i=left.tuplelist.get(0).tuple.length;i<newLength;i++){
                        tuple[i]=tempRight.tuple[i-left.tuplelist.get(0).tuple.length];
                        ids[i]=tempRight.tupleIds[i-left.tuplelist.get(0).tuple.length];
                    }
                    Tuple tempTuple = new Tuple();
                    tempTuple.tuple=tuple;
                    tempTuple.tupleIds=ids;
                    tupleList.addTuple(tempTuple);
                }
            }
        }
        return tupleList;
    }

    //对classTableItemList和tuplelist综合处理一下，获取selectResult
    public SelectResult getSelectResult(ArrayList<ClassTableItem> classTableItemList,TupleList tupleList){
        SelectResult selectResult=new SelectResult();
        selectResult.setClassName(new String[classTableItemList.size()]);
        selectResult.setAttrname(new String[classTableItemList.size()]);
        selectResult.setAttrid(new int[classTableItemList.size()]);
        selectResult.setType(new String[classTableItemList.size()]);
        selectResult.setAlias(new String[classTableItemList.size()]);
        for(int i=0;i<classTableItemList.size();i++){
            selectResult.getClassName()[i]=classTableItemList.get(i).classname;
            selectResult.getAlias()[i]=classTableItemList.get(i).alias;
            selectResult.getAttrid()[i]=classTableItemList.get(i).attrid;
            selectResult.getAttrname()[i]=classTableItemList.get(i).attrname;
            selectResult.getType()[i]=classTableItemList.get(i).attrtype;
        }
        selectResult.setTpl(tupleList);
        return selectResult;
    }

    //针对每个selectItem，得到其对应的column 比如a*b+c得到a，b，c
    public HashMap<SelectItem,ArrayList<Column>> getSelectItemColumn(ArrayList<SelectItem> selectItemArrayList){
        HashMap<SelectItem,ArrayList<Column>> res=new HashMap<>();
        for(SelectItem selectItem:selectItemArrayList){
            ArrayList<Column> columns=new ArrayList<>();
            getSelectColumn((SimpleNode) selectItem.getASTNode(),columns);
            res.put(selectItem,columns);
        }
        return res;
    }

    //递归遍历，直到当前元素是column为止
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

    //在selectItem中出现的所有的column
    public ArrayList<Column> getSelectColumnList(HashMap<SelectItem,ArrayList<Column>> map){
        ArrayList<Column> res=new ArrayList<>();
        for(ArrayList<Column> columns:map.values()){
            res.addAll(columns);
        }
        return res;
    }

    public ArrayList<ClassTableItem> getSelectItem(FromItem fromItem){
        ArrayList<ClassTableItem> elicitAttrItemList=new ArrayList<>();
        for(ClassTableItem item : memConnect.getClasst().classTable){
            //如果classTableItem中的className对上了fromItem就加入结果中
            if(item.classname.equals(((Table)fromItem).getName())){
                //硬拷贝，不然后续操作会影响原始信息。
                ClassTableItem temp=item.getCopy();
                //因为后续有许多针对alias的比对操作，所以，如果fromItem中使用了alias，则在classTableItem中的alias属性中存入该值
                if(fromItem.getAlias()!=null) temp.alias=fromItem.getAlias().getName();
                elicitAttrItemList.add(temp);
            }
        }
        return elicitAttrItemList;
    }

    public ArrayList<ClassTableItem> getSelectItem(FromItem fromItem, List<Column> columnList){
        // 从class表中提取将要获取的元素。
        ArrayList<ClassTableItem> elicitAttrItemList=new ArrayList<>();
        for(ClassTableItem item : memConnect.getClasst().classTable){
            if(item.classname.equals(fromItem.toString())){
                String attrName=item.attrname;
                boolean flag=false;
                for(Column column:columnList){
                    Column c=(Column) column;
                    if(c.getTable()!=null && !(c.getTable().equals(fromItem.toString())&& c.getTable().equals(fromItem.getAlias().getName()))) continue;
                    if(attrName.equals(c.getColumnName())) {
                        flag=true;
                        break;
                    }
                }
                if(flag) elicitAttrItemList.add(item);
            }
        }
        return elicitAttrItemList;
    }

    public int getClassId(String fromItem) throws TMDBException {
        for(ClassTableItem item : memConnect.getClasst().classTable) {
            if (item.classname.equals(fromItem)) {
                return item.classid;
            }
        }
        throw new TMDBException(fromItem+"表不存在");
//        return -1;
    }

    //输入需要获取的表名，得到对应的元祖值
    public TupleList getTable(FromItem fromItem) throws TMDBException {
        int classid=this.getClassId(((Table) fromItem).getName());
        TupleList res=new TupleList();
        for(ObjectTableItem item : memConnect.getTopt().objectTable) {
            if (item.classid == classid) {
                Tuple tuple = memConnect.GetTuple(item.tupleid);
                if(tuple!= null && tuple.delete==false){
                    tuple.setTupleId(item.tupleid);
                    res.addTuple(tuple);
                }
            }
        }
        return res;
    }

    public TupleList getTable(int classid) throws TMDBException {
        TupleList res=new TupleList();
        for(ObjectTableItem item : memConnect.getTopt().objectTable) {
            if (item.classid == classid) {
                Tuple tuple = memConnect.GetTuple(item.tupleid);
                if(tuple!= null && tuple.delete==false){
                    tuple.setTupleId(item.tupleid);
                    res.addTuple(tuple);
                }
//                Tuple newTuple=new Tuple();
//                newTuple.tuple=new Object[elicitAttrItemList.size()];
//                for(int i=0;i<elicitAttrItemList.size();i++){
//                    newTuple.tuple[i]=tuple.tuple[elicitAttrItemList.get(i).attrid];
//                }

            }
        }
        return res;
    }

}
