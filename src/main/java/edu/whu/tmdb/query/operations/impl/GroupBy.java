package edu.whu.tmdb.query.operations.impl;/*
 * className:GroupBy
 * Package:edu.whu.tmdb.Transaction.Transactions.impl
 * Description:
 * @Author: xyl
 * @Create:2023/8/6 - 20:09
 * @Version:v1
 */

import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.Tuple;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class GroupBy {
    private MemConnect memConnect;

    public GroupBy(MemConnect memConnect) {
        this.memConnect = memConnect;
    }

    public GroupBy() throws TMDBException {}

    public HashMap<Object, ArrayList<Tuple>> groupBy(PlainSelect plainSelect, SelectResult selectResult) throws TMDBException {
        return execute(plainSelect.getGroupBy(),selectResult);
    }

    private HashMap<Object, ArrayList<Tuple>> execute(GroupByElement groupBy, SelectResult selectResult) throws TMDBException {
        HashMap<Object, ArrayList<Tuple>> map = new HashMap<>();
        int i=groupByIndex(groupBy,selectResult.getAlias(),selectResult.getAttrname());
        if(i==-1){
            throw new TMDBException(/*"group by element doesn't exist"*/);
        }
        HashSet<String> groupByElement=new HashSet<>();
        for (Tuple tuple : selectResult.getTpl().tuplelist) {
            map.putIfAbsent(tuple.tuple[i],new ArrayList<>());
            map.get(tuple.tuple[i]).add(tuple);
        }
        return map;
    }

    private int groupByIndex(GroupByElement groupBy, String[] alias, String[] attrname) {
        String groupByElement = groupBy.getGroupByExpressionList().getExpressions().get(0).toString();
        for (int i = 0; i < alias.length; i++) {
            if (groupByElement.toString().equals(alias[i])) {
                return i;
            }
        }
        for (int i = 0; i < attrname.length; i++) {
            if(groupByElement.toString().equals(attrname[i])){
                return i;
            }
        }
        return -1;
    }


}
