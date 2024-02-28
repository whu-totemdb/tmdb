package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.query.operations.Exception.ErrorList;
import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.IOException;
import java.util.ArrayList;

import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Create;
import edu.whu.tmdb.query.operations.utils.MemConnect;

public class CreateImpl implements Create {

    public CreateImpl() {}

    @Override
    public boolean create(Statement stmt) throws TMDBException {
        return execute((CreateTable) stmt);
    }

    public boolean execute(CreateTable stmt) throws TMDBException {
        // 1.获取新定义class的属性列表和类名
        ArrayList<ColumnDefinition> columnDefinitionArrayList = (ArrayList<ColumnDefinition>) stmt.getColumnDefinitions();
        String classname = stmt.getTable().toString();

        // 2.判断类名的唯一性（要满足唯一性约束）
        for (ClassTableItem item : MemConnect.getClassTableList()) {
            if (item.classname.equals(classname)) {
                throw new TMDBException(ErrorList.TABLE_ALREADY_EXISTS, classname);
            }
        }

        // 3.新建class
        int count = columnDefinitionArrayList.size();
        MemConnect.getClassTable().maxid++;
        int classid = MemConnect.getClassTable().maxid;
        for (int i = 0; i < count; i++) {
            MemConnect.getClassTableList().add(new ClassTableItem(classname, classid, count, i,
                    columnDefinitionArrayList.get(i).getColumnName(), columnDefinitionArrayList.get(i).toStringDataTypeAndSpec()
                    ,"ori",""));
        }
        return true;
    }
}
