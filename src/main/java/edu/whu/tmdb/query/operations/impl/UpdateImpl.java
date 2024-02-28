package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.query.operations.Exception.ErrorList;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Select;
import edu.whu.tmdb.query.operations.Update;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;

public class UpdateImpl implements Update {

    private final MemConnect memConnect;

    public UpdateImpl() { this.memConnect = MemConnect.getInstance(MemManager.getInstance()); }

    @Override
    public void update(Statement stmt) throws JSQLParserException, TMDBException, IOException {
        execute((net.sf.jsqlparser.statement.update.Update) stmt);
    }

    public void execute(net.sf.jsqlparser.statement.update.Update updateStmt) throws JSQLParserException, TMDBException, IOException {
        // 1.update语句(类名/属性名)存在性检测
        String updateTableName = updateStmt.getTable().getName();
        if (!memConnect.classExist(updateTableName)) {
            throw new TMDBException(ErrorList.CLASS_NAME_DOES_NOT_EXIST, updateTableName);
        }
        ArrayList<UpdateSet> updateSetStmts = updateStmt.getUpdateSets();    // update语句中set字段列表
        for (UpdateSet updateSetStmt : updateSetStmts) {
            String columnName = updateSetStmt.getColumns().get(0).getColumnName();
            if (!memConnect.columnExist(updateTableName, columnName)) {
                throw new TMDBException(ErrorList.COLUMN_NAME_DOES_NOT_EXIST, columnName);
            }
        }

        // 2.获取符合where条件的所有元组
        String sql = "select * from " + updateTableName + " where " + updateStmt.getWhere().toString() + ";";
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sql.getBytes());
        net.sf.jsqlparser.statement.select.Select parse = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil.parse(byteArrayInputStream);
        Select select = new SelectImpl();
        SelectResult selectResult = select.select(parse);   // 注：selectResult均为临时副本，不是源数据

        // 3.执行update操作
        int[] indexs = new int[updateSetStmts.size()];      // update中set语句修改的属性->类表中属性的映射关系
        Object[] updateValue = new Object[updateSetStmts.size()];
        setMapping(selectResult.getAttrname(), updateSetStmts, indexs, updateValue);
        int classId = memConnect.getClassId(updateTableName);
        update(selectResult.getTpl(), indexs, updateValue, classId);
    }

    /**
     * update的具体执行过程
     * @param tupleList   经筛选得到的tuple list副本（只包含tuple属性）
     * @param indexs      update中set语句修改的属性->类表中属性的映射关系
     * @param updateValue set语句中的第i个对应于源类中第j个属性修改后的值
     * @param classId     修改表的id
     */
    public void update(TupleList tupleList, int[] indexs, Object[] updateValue, int classId) throws TMDBException {
        // 1.更新源类tuple
        ArrayList<Integer> updateIdList = new ArrayList<>();
        for (Tuple tuple : tupleList.tuplelist) {
            for (int i = 0; i < indexs.length; i++) {
                tuple.tuple[indexs[i]] = updateValue[i];
            }
            memConnect.UpateTuple(tuple, tuple.getTupleId());
            updateIdList.add(tuple.getTupleId());
        }

        // 2.根据biPointerTable找到对应的deputyTuple
        ArrayList<Integer> deputyTupleIdList = new ArrayList<>();
        TupleList deputyTupleList = new TupleList();    // 所有代理类的元组
        for (BiPointerTableItem biPointerTableItem : MemConnect.getBiPointerTableList()) {
            if (updateIdList.contains(biPointerTableItem.objectid)) {
                deputyTupleIdList.add(biPointerTableItem.deputyobjectid);
                Tuple tuple = memConnect.GetTuple(biPointerTableItem.deputyobjectid);
                deputyTupleList.addTuple(tuple);
            }
        }
        if (deputyTupleIdList.isEmpty()) { return; }

        // 3.获取deputyTupleId->...的哈希映射列表
        List<Integer> collect = Arrays.stream(indexs).boxed().collect(Collectors.toList());
        HashMap<Integer, ArrayList<Integer>> deputyId2AttrId = new HashMap<>();         // 满足where条件的deputyId -> deputyAttrIdList(其实也是index)
        HashMap<Integer, ArrayList<Object>> deputyId2UpdateValue = new HashMap<>();     // 满足where条件的deputyId -> 更新后的属性值列表(其实也是updateValue)
        for (SwitchingTableItem switchingTableItem : MemConnect.getSwitchingTableList()) {
            if (switchingTableItem.oriId == classId && collect.contains(switchingTableItem.oriAttrid)) {
                if (!deputyId2AttrId.containsKey(switchingTableItem.deputyId)) {
                    deputyId2AttrId.put(switchingTableItem.deputyId, new ArrayList<>());
                    deputyId2UpdateValue.put(switchingTableItem.deputyId, new ArrayList<>());
                }
                deputyId2AttrId.get(switchingTableItem.deputyId).add(switchingTableItem.deputyAttrId);
                int tempIndex = collect.indexOf(switchingTableItem.oriAttrid);
                deputyId2UpdateValue.get(switchingTableItem.deputyId).add(updateValue[tempIndex]);
            }
        }

        // 4.递归修改所有代理类
        for (int deputyId : deputyId2AttrId.keySet()) { // 遍历所有代理类id
            TupleList updateTupleList = new TupleList();
            for (Tuple tuple : deputyTupleList.tuplelist) {
                if (tuple.classId == deputyId) {
                    updateTupleList.addTuple(tuple);    // 找到该代理类的所有元组
                }
            }
            int[] nextIndexs = deputyId2AttrId.get(deputyId).stream().mapToInt(Integer -> Integer).toArray();
            Object[] nextUpdate = deputyId2UpdateValue.get(deputyId).toArray();
            update(updateTupleList, nextIndexs, nextUpdate, deputyId);
        }
    }

    /**
     * 给定attrNames和updateSetStmts，对indexs和updateValue进行赋值
     * @param attrNames 满足更新条件元组的属性名列表
     * @param updateSetStmts update语句set字段列表
     * @param indexs 赋值：set字段属性->元组属性的位置对应关系
     * @param updateValue 赋值：set字段赋值列表
     */
    private void setMapping(String[] attrNames, ArrayList<UpdateSet> updateSetStmts, int[] indexs, Object[] updateValue) {
        for (int i = 0; i < updateSetStmts.size(); i++) {
            UpdateSet updateSet = updateSetStmts.get(i);
            for (int j = 0; j < attrNames.length; j++) {
                if (!updateSet.getColumns().get(0).getColumnName().equals(attrNames[j])) { continue; }

                // 如果set的属性在元组属性列表中，进行赋值
                if (updateSet.getExpressions().get(0) instanceof StringValue) {
                    updateValue[i] = ((StringValue) updateSet.getExpressions().get(0)).getValue();
                } else {
                    updateValue[i] = updateSet.getExpressions().get(0).toString();
                }
                indexs[i] = j;      // set语句中的第i个对应于源类中第j个属性
                break;
            }
        }
    }

}
