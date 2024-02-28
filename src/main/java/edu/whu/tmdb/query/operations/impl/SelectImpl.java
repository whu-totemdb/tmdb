package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.query.operations.Exception.ErrorList;
import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.Formula;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;

//1、from子句组装来自不同数据源的数据；
//2、where子句基于指定的条件对记录行进行筛选；
//3、group by子句将数据划分为多个分组；
//4、使用聚集函数进行计算；
//5、使用having子句筛选分组；
//6、计算所有的表达式；
//7、select 的字段；
//8、使用order by对结果集进行排序。
public class SelectImpl implements edu.whu.tmdb.query.operations.Select {

    private final MemConnect memConnect;

    public SelectImpl() { this.memConnect = MemConnect.getInstance(MemManager.getInstance()); }

    @Override
    public SelectResult select(Object stmt) throws TMDBException, IOException {
        SelectBody selectBody = null;
        if (stmt.getClass().getSimpleName().equals("Select")) {         // 不带子查询的类型转换
            selectBody = ((net.sf.jsqlparser.statement.select.Select)stmt).getSelectBody();
        }else if (stmt.getClass().getSimpleName().equals("SubSelect")) {// 带子查询的类型转换
            selectBody = ((SubSelect)stmt).getSelectBody();
        }

        SelectResult res = new SelectResult();
        assert selectBody != null;
        if ((selectBody.getClass().getSimpleName().equals("SetOperationList"))) {
            // 带union，except关键字的查询
            SetOperationList setOperationList = (SetOperationList) selectBody;
            return setOperation(setOperationList);
        }else {     // 简单查询
            res = plainSelect(selectBody);
        }
        return res;
    }

    // 简单查询的执行过程
    public SelectResult plainSelect(SelectBody stmt) throws TMDBException, IOException {
        // Values 也是一种plainSelect，如果是values，由values方法处理
        if(stmt.getClass().getSimpleName().equals("ValuesStatement")) {
            return values((ValuesStatement) stmt);
        }

        PlainSelect plainSelect = (PlainSelect) stmt;
        if (plainSelect.isDeputySelect()) {     // 对象代理的跨类查询
            return deputySelect((PlainSelect) stmt);
        }

        // 以下是常规plainselect逻辑：from->where->select
        // 1.调用from方法获取相关的所有元数据
        SelectResult selectResult = from(plainSelect);
        // 2.调用where对元数据进行进行筛选
        if (plainSelect.getWhere() != null){
            Where where = new Where();
            selectResult = where.where(plainSelect, selectResult);
        }
        if (plainSelect.getLimit() != null){
            selectResult = limit(Integer.parseInt(plainSelect.getLimit().getRowCount().toString()), selectResult);
        }
        if(plainSelect.getGroupBy() != null){
            GroupBy groupBy = new GroupBy();
            HashMap<Object, ArrayList<Tuple>> hashMap = groupBy.groupBy(plainSelect, selectResult);
            selectResult = groupByElicit(plainSelect, hashMap, selectResult);
        }
        else {//然后通过selectItem提取想要的列
            selectResult = projection(plainSelect, selectResult);
        }
        //最终返回selectResult
        return selectResult;
    }

    private SelectResult limit(int limit,SelectResult selectResult) {
        // TODO-task9
        return selectResult;
    }

    private SelectResult groupByElicit(PlainSelect plainSelect, HashMap resultMap, SelectResult selectResult) throws TMDBException {
        String groupByElement = plainSelect.getGroupBy().getGroupByExpressionList().getExpressions().get(0).toString();
        ArrayList<SelectItem> selectItemList= (ArrayList<SelectItem>) plainSelect.getSelectItems();
        HashMap<SelectItem, ArrayList<Column>> map = getSelectItemColumn(selectItemList);
        //select item的length
        int length=selectItemList.size();

        //init resTupleList
        TupleList resTupleList=new TupleList();
        SelectResult result=new SelectResult();
        result.setAlias(new String[length]);
        result.setAttrname(new String[length]);
        result.setClassName(new String[length]);
        result.setAttrid(new int[length]);
        result.setType(new String[length]);
        for(int i=0;i<resultMap.size();i++){
            Tuple tuple=new Tuple();
            int[] temp=new int[length];
            Arrays.fill(temp,-1);
            tuple.tupleIds=temp;
            tuple.tuple=new Object[length];
            resTupleList.addTuple(tuple);
        }
        result.setTpl(resTupleList);
        int i=0;
        //按照列处理res Result
        while(i<length){
//            这里是针对selectItem是表达式的判定，a或者a+2或者a+b*c都会被认定为表达式
            if(selectItemList.get(i).getClass().getSimpleName().equals("SelectExpressionItem")){
                SelectExpressionItem selectItem=(SelectExpressionItem) selectItemList.get(i);
                //如果有alias 例如a*b as c则需要将输出的getAttrname()改成别名
                if(selectItem.getAlias()!=null){
                    result.getAlias()[i]=selectItem.getAlias().getName();
                    result.getAttrname()[i]=selectItem.getAlias().getName();
                }
                else {
                    result.getAlias()[i]=selectItem.toString();
                    result.getAttrname()[i]=selectItem.toString();
                }
                ArrayList<Object> thisColumn=new ArrayList<>();
                if(selectItem.getExpression().toString().equals(groupByElement)
                ||(selectItem.getAlias()!=null && selectItem.getAlias().getName().equals(groupByElement))){
                    for (Object o :
                            resultMap.keySet()) {
                        thisColumn.add(o);
                    }
                }
                else{
                    Function func = (Function) selectItem.getExpression();
                    String funcName = func.getName();
                    String p = func.getParameters().getExpressions().get(0).toString();
                    int pIndex=-1;
                    for (int j = 0; j < selectResult.getAttrname().length; j++) {
                        if(p.equals(selectResult.getAttrname()[j])
                        || p.equals(selectResult.getAlias()[j])){
                            pIndex=j;
                        }
                    }
                    if(pIndex==-1){
                        throw new TMDBException(/*2, p*/);
                    }
                    thisColumn=solveAggregationFunction(resultMap,funcName,pIndex);
                }
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
            }
        }

        return result;
    }

    private ArrayList<Object> solveAggregationFunction(HashMap<Object,ArrayList<Tuple>> resultMap, String funcName, int pIndex) {
        ArrayList<Object> res = new ArrayList<>();
        for (Object k :
                resultMap.keySet()) {
            ArrayList<Tuple> tuples = resultMap.get(k);
            List<Double> temp=new ArrayList<Double>();
            for (Tuple t:
                 tuples) {
                if(t.tuple[pIndex]!=null) {
                    temp.add(Double.parseDouble((String) t.tuple[pIndex]));
                }
            }
            double d=solveList(funcName,temp);
            res.add(d);
        }
        return res;
    }

    private double solveList(String funcName, List<Double> temp) {
        switch (funcName.toLowerCase()) {
            case "avg":
                return temp.stream()
                        .mapToDouble(Double::doubleValue)
                        .average()
                        .orElse(0);
            case "min":
                return temp.stream()
                        .mapToDouble(Double::doubleValue)
                        .min()
                        .orElse(0);
            case "max":
                return temp.stream()
                        .mapToDouble(Double::doubleValue)
                        .max()
                        .orElse(0);
            case "count":
                return temp.size();
            case "sum":
                return temp.stream()
                        .mapToDouble(Double::doubleValue)
                        .sum();
            default: return -1;
        }
    }

    /**
     * projection操作，从当前元组列表中挑选出想要的的属性列
     * @param plainSelect 子查询语句
     * @param entireResult 根据from语句得到，子查询的涉及的所有满足where条件的完整元组
     * @return 挑选之后的属性列表
     */
    public SelectResult projection(PlainSelect plainSelect, SelectResult entireResult) throws TMDBException {
        ArrayList<SelectItem> selectItemList = (ArrayList<SelectItem>) plainSelect.getSelectItems();    // 获取from后的查询项 (形如from id, value as v)
        int resultSize = getSelectResultSize(selectItemList);           // 对应查询结果的列数
        SelectResult projectResult = new SelectResult(resultSize);      // 存储返回的结果
        TupleList resTupleList = new TupleList(resultSize, entireResult.getTpl().tuplelist.size());     // 存储最终projection结果的tupleList

        int indexInResult = 0;  // 对应于projectResult的下标

        // 双指针遍历赋值
        for (SelectItem item : selectItemList) {
            // 1.针对select * from
            if (item.getClass().getSimpleName().equals("AllColumns")) {
                return projectAllColums(entireResult);
            }
            // 2.针对select a from或者select a+b as c from test (认定为表达式)
            if (item.getClass().getSimpleName().equals("SelectExpressionItem")) {
                projectSelectExpression(item, entireResult, projectResult, resTupleList, indexInResult);
                indexInResult++;
            }
            // 3.针对select a.* from a,b
            if (item.getClass().getSimpleName().equals("AllTableColumns")){
                indexInResult = projectAllTableColumns(item, entireResult, projectResult, resTupleList, indexInResult);
            }
        }
        projectResult.setTpl(resTupleList);
        return projectResult;
    }

    /**
     * 根据查询项列表确定查询结果的列数
     * @param selectItemList 查询项列表
     * @return 查询结果的列数
     */
    private int getSelectResultSize(ArrayList<SelectItem> selectItemList) throws TMDBException {
        int selectResultSize = 0;
        for (SelectItem item : selectItemList) {
            // 1.针对select * from
            if (item.getClass().getSimpleName().equals("AllColumns")) {
                return 1;
            }
            // 2.针对select a from或者select a+b as c from test (认定为表达式)
            if (item.getClass().getSimpleName().equals("SelectExpressionItem")) {
                selectResultSize++;
            }
            // 3.针对select className.* from
            if (item.getClass().getSimpleName().equals("AllTableColumns")) {
                AllTableColumns selectItem = (AllTableColumns) item;
                Table table = selectItem.getTable();
                selectResultSize += memConnect.getClassAttrnum(table.getName());
            }
        }
        return selectResultSize;
    }

    /**
     * 针对select * from子句，用于处理 "AllColumns" 类型的投影操作
     * @param entireResult 根据from语句得到，子查询的涉及的所有满足where条件的完整元组
     * @return project结果，即select语句最终结果
     */
    private SelectResult projectAllColums(SelectResult entireResult) {
        entireResult.setAlias(entireResult.getAttrname());
        // 要对attrid重拍以下，不然最终printResult的时候会有问题
        for (int j = 0; j < entireResult.getAttrid().length; j++) {
            entireResult.getAttrid()[j] = j;
        }
        return entireResult;
    }

    /**
     * 针对select a from或者select a+b as c from test (认定为表达式)执行projection
     * @param item 查询项 (如：[a+b as c])
     * @param entireResult 根据from语句得到，子查询的涉及的所有满足where条件的完整元组
     * @param projectResult project结果，即select语句最终结果
     * @param resTupleList 存储全部projection元组的list
     * @param indexInResult projectResult的赋值下标，表示查询结果的列数
     */
    private void projectSelectExpression(SelectItem item, SelectResult entireResult,
                SelectResult projectResult, TupleList resTupleList, int indexInResult) throws TMDBException {
        // TODO-task5
        SelectExpressionItem selectItem = (SelectExpressionItem) item;
        // 1.attrName赋值

        // 2.alias赋值

        // 3.resTupleList赋值
        // ArrayList<String> TableColumn = new ArrayList<>();          // 含有两个元素的列表，结构为[tableName, columnName]
        // 调用attributeParser();
        // ArrayList<Object> dataList = (new Formula()).formulaExecute(selectItem.getExpression(), entireResult);  // 对表达式进行解析，获取该列的值
        // 调用getIndexInEntireResult();   // 找到表达式对应属性在原元组对应的下标

        // 4.剩余属性赋值

    }

    /**
     * 针对select a.* from a,b执行projection
     * @param item 查询项，如 a.*
     * @param entireResult 根据from语句得到，子查询的涉及的所有满足where条件的完整元组
     * @param projectResult project结果，即select语句最终结果
     * @param resTupleList 存储全部projection元组的list
     * @param indexInResult projectResult的赋值下标，表示本次赋值的起始的列数
     * @return indexInResult 下次赋值的起始的列数
     */
    private int projectAllTableColumns(SelectItem item, SelectResult entireResult, SelectResult projectResult,
                                       TupleList resTupleList, int indexInResult) {
        AllTableColumns selectItem = (AllTableColumns) item;
        Table table = selectItem.getTable();

        // 遍历所有的数据列
        for (int i = 0; i < entireResult.getClassName().length; i++){
            String className = entireResult.getClassName()[i];
            String alias = entireResult.getAlias()[i];
            // 将选取表的所有列加入结果集合中
            if (className.equals(table.getName()) || table.getAlias() != null && table.getAlias().getName().equals(alias)) {
                projectResult.getAttrname()[indexInResult] = entireResult.getAttrname()[i];
                projectResult.getAlias()[indexInResult] = entireResult.getAttrname()[i];
                projectResult.getType()[indexInResult] = entireResult.getType()[i];
                projectResult.getAttrid()[indexInResult] = indexInResult;
                for (int j = 0; j < resTupleList.tuplelist.size(); j++){
                    resTupleList.tuplelist.get(j).tuple[indexInResult] = entireResult.getTpl().tuplelist.get(j).tuple[i];
                    resTupleList.tuplelist.get(j).tupleIds[indexInResult] = entireResult.getTpl().tuplelist.get(j).tupleIds[i];
                }
                indexInResult++;
            }
        }
        return indexInResult;
    }

    /**
     * 表达式解析函数，给定表达式（形如value1 + value2 as sum）
     * 将其第一个属性的表名和属性名赋值给相应变量
     * @param expression 其实是selectItem，a.value1 + b.value2 as sum
     * @param TableColumn 包含两个值：第一个表达式的表名，有则赋值为"a"，没有赋值为""；第一个表达式的属性名
     */
    private void attributeParser(String expression, ArrayList<String> TableColumn) {
        String attr;
        int spaceIndex = expression.indexOf(' ');
        if (spaceIndex != -1) {
            attr = expression.substring(0, spaceIndex);
        } else {
            // 处理没有空格的情况，可以使用整个字符串或者其他默认值
            attr = expression;
        }
        if (attr.contains(".")) {
            // 如果存在"."，则分割字符串
            String[] parts = attr.split("\\.", 2);
            TableColumn.add(parts[0]);
            TableColumn.add(parts[1]);
        } else {
            // 如果不存在"."，整个字符串赋值给attrName，tableName为空字符串
            TableColumn.add("");
            TableColumn.add(attr);
        }
    }

    /**
     * 给定查询属性的属性名和表名，返回属性在完全的查询结果中的位置
     * @param entireResult 完全查询结果
     * @param tableName 待查询表名
     * @param columnName 待查询属性名
     * @return 待查询在完全查询列表中的下标index
     */
    private int getIndexInEntireResult(SelectResult entireResult, String tableName, String columnName) {
        for (int i = 0; i < entireResult.getClassName().length; i++) {
            if (entireResult.getAttrname()[i].equals(columnName)) {
                if (tableName.equals("")) {
                    return i;
                }
                if (entireResult.getClassName()[i].equals(tableName) || entireResult.getAlias()[i].equals(tableName)) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * 获取查询语句中涉及的所有元数据
     * @param plainSelect 平凡查询语句
     * @return 查询语句中涉及的所有元数据
     */
    public SelectResult from(PlainSelect plainSelect) throws TMDBException {
        FromItem fromItem = plainSelect.getFromItem();              // 获取plainSelect的表（多表查询时取第一个table）
        if (fromItem == null) { throw new TMDBException(ErrorList.MISSING_FROM_CLAUSE ); }
        TupleList tupleList = memConnect.getTupleList(fromItem);    // 获取from后面table的所有元组
        ArrayList<ClassTableItem> classTableItemList = memConnect.copyClassTableList(fromItem);     // 获取class item信息，对应于select输出列表的表头
        SelectResult selectResult = getSelectResult(classTableItemList, tupleList);

        // 进行join操作
        if(!(plainSelect.getJoins() == null)){
            for (Join join:plainSelect.getJoins()) {
                // 获取当前join表的的一些元祖
                ArrayList<ClassTableItem> tempClassTableItem = memConnect.copyClassTableList(join.getRightItem());
                TupleList tempTupleList = memConnect.getTupleList(join.getRightItem());
                SelectResult tempSelectResult = getSelectResult(tempClassTableItem, tempTupleList);

                // 将本来的TupleList和当前操作的join的tuplelist根据join的形式进行组合
                tupleList = join(selectResult,tempSelectResult,join);
                // 把classTableItem进行合并
                classTableItemList.addAll(tempClassTableItem);
                // 根据join后的tuple和合并后的classtableItemlist生成selectResult
                selectResult=getSelectResult(classTableItemList, tupleList);
            }
        }
        return selectResult;
    }

    //跨类查询。。。。
    public SelectResult deputySelect(PlainSelect plainSelect) throws TMDBException {
        ArrayList<SelectItem> selectItemList=(ArrayList<SelectItem>)plainSelect.getSelectItems();
        FromItem fromItem=plainSelect.getFromItem();
        TupleList tupleList= memConnect.getTupleList(fromItem);
        HashMap<SelectItem,ArrayList<Column>> selectItemToColumn=getSelectItemColumn(selectItemList);
        List<Column> selectColumnList=getSelectColumnList(selectItemToColumn);
        ArrayList<ClassTableItem> classTableItemList=this.getSelectItem(fromItem,selectColumnList);
        return getSelectResult(classTableItemList,tupleList);
    }

    //进行union这种操作的方法
    public SelectResult setOperation(SetOperationList setOperationList) throws TMDBException, IOException {
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
        if(selectResult1.getAttrname().length!=selectResult2.getAttrname().length) throw new TMDBException();
        //根据setOperation的种类进行操作划分
        switch (setOperation.toString()){
            case "UNION":       return union(selectResult1, selectResult2);
            case "INTERSECT":   return intersect(selectResult1, selectResult2);
            case "EXCEPT":      return except(selectResult1, selectResult2);
            case "MINUS":       return minus(selectResult1, selectResult2);
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
        for (Expression value : expressions) {
            if (value.getClass().getSimpleName().equals("RowConstructor")) {
                //values按照行进行存储
                RowConstructor rowConstructor = (RowConstructor) value;
                ExpressionList expressionList1 = rowConstructor.getExprList();
                Object[] tuple = new Object[expressionList1.getExpressions().size()];
                //将每行的值传到新建的tuple中
                for (int j = 0; j < expressionList1.getExpressions().size(); j++) {
                    tuple[j] = expressionList1.getExpressions().get(j).toString();
                }
                tupleList.addTuple(new Tuple(tuple));
            } else if (value.getClass().getSimpleName().equals("Parenthesis")) {
                Parenthesis parenthesis = (Parenthesis) value;
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
            if(leftIndex==-1) throw new TMDBException(ErrorList.COLUMN_NAME_DOES_NOT_EXIST, leftExpression.getColumnName());
            for(int i=0;i<right.getAttrname().length;i++){
                if(rightExpression.getColumnName().equals(right.getAttrname()[i])){
                    rightIndex=i;
                    break;
                }
            }
            if(rightIndex==-1) throw new TMDBException(ErrorList.COLUMN_NAME_DOES_NOT_EXIST, rightExpression.getColumnName());
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

    /**
     * 给定表项和所有的元组，将其包装成selectResult类。用于获取select表的所有元组
     * @param classTableItemList class item，对应查询结果的表头
     * @param tupleList 元组列表，对应查询结果的元数据
     * @return 包含所有查询元组的selectResult类
     */
    public SelectResult getSelectResult(ArrayList<ClassTableItem> classTableItemList, TupleList tupleList){
        SelectResult selectResult = new SelectResult(classTableItemList.size());
        for(int i = 0; i < classTableItemList.size();i++){
            selectResult.getClassName()[i] = classTableItemList.get(i).classname;
            selectResult.getAlias()[i] = classTableItemList.get(i).alias;
            selectResult.getAttrid()[i] = classTableItemList.get(i).attrid;
            selectResult.getAttrname()[i] = classTableItemList.get(i).attrname;
            selectResult.getType()[i] = classTableItemList.get(i).attrtype;
        }
        selectResult.setTpl(tupleList);
        return selectResult;
    }

    /**
     * 针对每个selectItem，得到其对应的column 比如a*b+c得到a，b，c
     * @param selectItemArrayList select语句之后的SelectItem列表
     * @return 查询项->使用属性列表的hashmap
     */
    public HashMap<SelectItem, ArrayList<Column>> getSelectItemColumn(ArrayList<SelectItem> selectItemArrayList){
        HashMap<SelectItem, ArrayList<Column>> res = new HashMap<>();
        for (SelectItem selectItem : selectItemArrayList) {
            ArrayList<Column> columns = new ArrayList<>();
            getSelectColumn((SimpleNode) selectItem.getASTNode(), columns);
            res.put(selectItem,columns);
        }
        return res;
    }

    /**
     * 给定SelectItem的语法树节点，将节点所包含的属性名加入到selectColumnList
     * @param node SelectItem的语法树节点
     * @param selectColumnList 赋值：SelectItem的语法树节点所包含的属性名
     */
    public void getSelectColumn(SimpleNode node, ArrayList<Column> selectColumnList){
        int i = 0;
        while (i < node.jjtGetNumChildren()) {
            SimpleNode currentNode = (SimpleNode) node.jjtGetChild(i);
            String className = currentNode.toString();
            if (className.equals("Column")) {
                selectColumnList.add((Column)currentNode.jjtGetValue());
            }
            if (currentNode.jjtGetNumChildren() > 0) {
                getSelectColumn((SimpleNode) currentNode,selectColumnList);
            }
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

    public ArrayList<ClassTableItem> getSelectItem(FromItem fromItem, List<Column> columnList){
        // 从class表中提取将要获取的元素。
        ArrayList<ClassTableItem> elicitAttrItemList=new ArrayList<>();
        for(ClassTableItem item : MemConnect.getClassTableList()){
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

}
