package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import drz.tmdb.Memory.Tuple;
import drz.tmdb.Memory.TupleList;

public class Insert {
    public Insert(){}

    public ArrayList<Integer> insert(Statement stmt){
        return execute((net.sf.jsqlparser.statement.insert.Insert)stmt);
    }

//    public boolean executor()
    public ArrayList<Integer> execute(net.sf.jsqlparser.statement.insert.Insert stmt){
//        ArrayList<ClassTableItem> classTableItems=memConnect.getSelectItem(table,columns);
        //获取表名
        Table table=stmt.getTable();
        //获取插入的column的名称
        List<Column> columns=stmt.getColumns();
        Select select=new Select();
        //insert后面的values是一个select节点，获取values或者其它类型select的值
        SelectResult selectResult=select.select(stmt.getSelect());
        //tuplelist就是SelectResult中实际存储tuple部分
        TupleList tupleList=selectResult.getTpl();
        //获取插入表的表名
        String tablename=table.getName();
        ArrayList<Integer> integers = new ArrayList<>();
        for(int i=0;i<tupleList.tuplenum;i++){
            Tuple tuple=tupleList.tuplelist.get(i);
            //创建memConnect中的tupleInsert需要的字符串数组
            String[] p=new String[tuple.tuple.length+3];
            p[0]="";
            p[1]=""+tuple.tuple.length;
            p[2]=tablename;
            //循环的将每个values（或者其它类型的）的值调用memConnect进行插入
            for(int j=0;j<tuple.tuple.length;j++){
                p[j+3]=tuple.tuple[j].toString();
            }
            integers.add(new MemConnect().tupleInsert(p));
        }
        return integers;
    }

    //INSERT INTO aa VALUES (1,2,"3");
    //4,3,aa,1,2,"3"
    //0 1 2  3 4  5

}
