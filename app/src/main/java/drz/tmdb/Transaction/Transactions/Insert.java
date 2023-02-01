package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;

import drz.tmdb.Memory.Tuple;
import drz.tmdb.Memory.TupleList;

public class Insert {
    private static MemConnect memConnect=new MemConnect();
    public Insert(){}

    public boolean insert(Statement stmt){
        stmt=(net.sf.jsqlparser.statement.insert.Insert)stmt;
        Table table=((net.sf.jsqlparser.statement.insert.Insert) stmt).getTable();
        List<Column> columns=((net.sf.jsqlparser.statement.insert.Insert) stmt).getColumns();
        Select select=new Select();
        SelectResult selectResult=select.select(((net.sf.jsqlparser.statement.insert.Insert) stmt).getSelect());
        return execute(table,columns,selectResult);
    }

//    public boolean executor()
    public boolean execute(Table table, List<Column> columns, SelectResult selectResult){
//        ArrayList<ClassTableItem> classTableItems=memConnect.getSelectItem(table,columns);
        TupleList tupleList=selectResult.getTpl();
        String tablename=table.getName();
        for(int i=0;i<tupleList.tuplenum;i++){
            Tuple tuple=tupleList.tuplelist.get(i);
            String[] p=new String[tuple.tuple.length+3];
            p[0]="";
            p[1]=""+tuple.tuple.length;
            p[2]=tablename;
            for(int j=0;j<tuple.tuple.length;j++){
                p[j+3]=tuple.tuple[j].toString();
            }
            memConnect.tupleInsert(p);
        }
        return true;
    }

    //INSERT INTO aa VALUES (1,2,"3");
    //4,3,aa,1,2,"3"
    //0 1 2  3 4  5

}
