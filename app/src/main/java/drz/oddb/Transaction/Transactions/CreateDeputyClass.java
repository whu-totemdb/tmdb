package drz.oddb.Transaction.Transactions;


import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.ArrayList;

public class CreateDeputyClass {
    private static MemConnect memConnect=new MemConnect();
    public CreateDeputyClass(){}

    public boolean createDeputyClass(Statement stmt){
        return execute((net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass) stmt);
    }

    public boolean execute(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt){
        String deputyClass=stmt.getDeputyClass().toString();
        Select select=new Select();
        SelectResult selectResult=select.select(stmt.getSelect());
        ArrayList<FromItem> fromItemArrayList=this.getOrginClass(stmt.getSelect());
        int[] originClassId=new int[fromItemArrayList.size()];
        for(int i=0;i<fromItemArrayList.size();i++){
            originClassId[i]=memConnect.getClassId(fromItemArrayList.get(i).toString());
        }

        return true;
    }

    public ArrayList<FromItem> getOrginClass(net.sf.jsqlparser.statement.select.Select select){
        PlainSelect plainSelect= (PlainSelect) select.getSelectBody();
        FromItem fromItem=plainSelect.getFromItem();
        ArrayList<FromItem> res=new ArrayList<>();
        res.add(fromItem);
        for(Join join:plainSelect.getJoins()){
            res.add(join.getRightItem());
        }
        return res;
    }
}
