package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Insert;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;

public class InsertImpl implements Insert {
    private MemConnect memConnect;

    ArrayList<Integer> tupleIdList = new ArrayList<>();

    public InsertImpl() {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public ArrayList<Integer> insert(Statement stmt) throws TMDBException, IOException {
        net.sf.jsqlparser.statement.insert.Insert insertStmt = (net.sf.jsqlparser.statement.insert.Insert) stmt;
        Table table = insertStmt.getTable();        // 解析insert对应的表
        List<String> attrNames = new ArrayList<>(); // 解析插入的字段名
        if (insertStmt.getColumns() == null){
            attrNames = memConnect.getColumns(table.getName());
        }
        else{
            int insertColSize = insertStmt.getColumns().size();
            for (int i = 0; i < insertColSize; i++) {
                attrNames.add(insertStmt.getColumns().get(i).getColumnName());
            }
        }

        // 对应含有子查询的插入语句
        SelectImpl select = new SelectImpl();
        SelectResult selectResult = select.select(insertStmt.getSelect());

        // tuplelist存储需要插入的tuple部分
        TupleList tupleList = selectResult.getTpl();
        execute(table.getName(), attrNames, tupleList);
        return tupleIdList;
    }

    /**
     * @param tableName 表名/类名
     * @param columns 表/类所具有的属性列表
     * @param tupleList 要插入的元组列表
     */
    public void execute(String tableName, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int classId = memConnect.getClassId(tableName);         // 类id
        int attrNum = memConnect.getClassAttrnum(tableName);    // 属性的数量
        int[] attrIdList = memConnect.getAttridList(classId, columns);         // 插入的属性对应的attrid列表
        for (Tuple tuple : tupleList.tuplelist) {
            if (tuple.tuple.length != columns.size()){
                throw new TMDBException(/*"Insert error: columns size doesn't match tuple size"*/);
            }
            tupleIdList.add(insert(classId, columns, tuple, attrNum, attrIdList));
        }
    }

    /**
     * @param classId 表/类id
     * @param columns 表/类所具有的属性列表
     * @param tupleList 要插入的元组列表
     */
    public void execute(int classId, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int attrNum = memConnect.getClassAttrnum(classId);
        int[] attrIdList = memConnect.getAttridList(classId, columns);
        for (Tuple tuple : tupleList.tuplelist) {
            if (tuple.tuple.length != columns.size()){
                throw new TMDBException(/*"Insert error: columns size doesn't match tuple size"*/);
            }
            tupleIdList.add(insert(classId, columns, tuple, attrNum, attrIdList));
        }
    }

    /**
     * @param classId 要插入的类id
     * @param columns 要插入类的属性名列表
     * @param tuple 要insert的元组tuple
     * @return 新插入元组的tuple id
     */
    public int execute(int classId, List<String> columns, Tuple tuple) throws TMDBException, IOException {
        int attrNum = memConnect.getClassAttrnum(classId);
        int[] attridList = memConnect.getAttridList(classId, columns);

        if (tuple.tuple.length != columns.size()){
            throw new TMDBException(/*"Insert error: columns size doesn't match tuple size"*/);
        }
        int tupleId = insert(classId, columns, tuple, attrNum, attridList);
        tupleIdList.add(tupleId);
        return tupleId;
    }


    /**
     * @param classId 插入表/类对应的id
     * @param columns 表/类所具有的属性名列表（来自insert语句）
     * @param tuple 要插入的元组
     * @param attrNum 元组包含的属性数量（系统表中获取）
     * @param attrId 插入属性对应的attrId列表（根据insert的属性名，系统表中获取）
     * @return 新插入属性的tuple id
     */
    private Integer insert(int classId, List<String> columns, Tuple tuple, int attrNum, int[] attrId) throws TMDBException, IOException {
        // 1.直接在对应类中插入tuple
        // 1.1 获取新插入元组的id
        int tupleid = MemConnect.getObjectTable().maxTupleId++;

        // 1.2 将tuple转换为可插入的形式
        Object[] temp = new Object[attrNum];
        for (int i = 0; i < attrId.length; i++) {
            temp[attrId[i]] = tuple.tuple[i];
        }
        tuple.setTuple(tuple.tuple.length, tupleid, classId, temp);

        // 1.3 元组插入操作
        memConnect.InsertTuple(tuple);
        MemConnect.getObjectTableList().add(new ObjectTableItem(classId, tupleid));

        // 2.找到所有的代理类，进行递归插入
        // 2.1 找到源类所有的代理类
        ArrayList<Integer> DeputyIdList = memConnect.getDeputyIdList(classId);

        // 2.2 将元组转换为代理类应有的形式
        if (!DeputyIdList.isEmpty()) {
            for (int deputyCalssId : DeputyIdList) {
                HashMap<String, String> attrNameHashMap = getAttrNameHashMap(classId, deputyCalssId, columns);
                List<String> deputyColumns = getDeputyColumns(attrNameHashMap, columns);    // 根据源类属性名列表获取代理类属性名列表
                Tuple deputyTuple = getDeputyTuple(attrNameHashMap, tuple, columns);        // 将插入源类的元组tuple转换为插入代理类的元组deputyTuple

                // 2.3 递归插入
                int tupleId = execute(deputyCalssId, deputyColumns, deputyTuple);
                MemConnect.getBiPointerTableList().add(new BiPointerTableItem(classId, tupleid, deputyCalssId, tupleId));
            }
        }
        return tupleid;
    }

    /**
     * 获取源类属性列表->代理类属性列表的哈希映射列表（注：可能有的源类属性不在代理类中）
     * @param originClassId 源类的class id
     * @param deputyClassId 代理类的class id
     * @param originColumns 源类属性名列表
     * @return 源类属性列表->代理类属性列表的哈希映射列表
     */
    private HashMap<String, String> getAttrNameHashMap(int originClassId, int deputyClassId, List<String> originColumns) {
        HashMap<String, String> attrNameHashMap = new HashMap<>();
        for (SwitchingTableItem switchingTableItem : MemConnect.getSwitchingTableList()) {
            if (switchingTableItem.oriId != originClassId || switchingTableItem.deputyId != deputyClassId) {
                continue;
            }

            for (String originColumn : originColumns) {
                if (switchingTableItem.oriAttr.equals(originColumn)) {
                    attrNameHashMap.put(originColumn, switchingTableItem.oriAttr);
                }
            }
        }
        return attrNameHashMap;
    }

    /**
     * 给定源类属性名列表，获取其代理类对应属性名列表（注：源类中有的属性可能不在代理类中）
     * @param attrNameHashMap 源类属性名->代理类属性名的哈希表
     * @param originColumns 源类属性名列表
     * @return 代理类属性名列表（注：源类中有的属性可能不在代理类中）
     */
    private List<String> getDeputyColumns(HashMap<String, String> attrNameHashMap, List<String> originColumns) {
        List<String> deputyColumns = new ArrayList<>();
        for (String originColumn : originColumns) {
            if (attrNameHashMap.containsKey(originColumn)){
                deputyColumns.add(attrNameHashMap.get(originColumn));
            }
        }
        return deputyColumns;
    }

    /**
     * 将插入源类的元组转换为插入代理类的元组
     * @param attrNameHashMap 源类属性名->代理类属性名的哈希表
     * @param originTuple 插入源类中的tuple
     * @param originColumns 源类属性名列表
     * @return 能够插入代理类的tuple
     */
    private Tuple getDeputyTuple(HashMap<String, String> attrNameHashMap, Tuple originTuple, List<String> originColumns) {
        Tuple deputyTuple = new Tuple();
        Object[] temp = new Object[attrNameHashMap.size()];
        int i = 0;
        for(String originColumn : originColumns){
            temp[i] = originTuple.tuple[originColumns.indexOf(originColumn)];
            i++;
        }
        deputyTuple.tuple = temp;
        return deputyTuple;
    }
}
