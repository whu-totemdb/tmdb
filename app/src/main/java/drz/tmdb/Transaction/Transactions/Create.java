package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.ArrayList;

public class Create {
    private static MemConnect memConnect=new MemConnect();
    public Create(){}

    public boolean create(Statement stmt){
        return execute((CreateTable) stmt);
    }

    //CREATE CLASS dZ123 (nB1 int,nB2 char) ;
    //1,2,dZ123,nB1,int,nB2,char
    public boolean execute(CreateTable stmt){
        ArrayList<ColumnDefinition> columnDefinitionArrayList= (ArrayList<ColumnDefinition>) stmt.getColumnDefinitions();
        String[] p=new String[columnDefinitionArrayList.size()*2+3];
        p[0]="";
        p[1]=""+columnDefinitionArrayList.size();
        p[2]=stmt.getTable().toString();
        for(int i=0;i<columnDefinitionArrayList.size();i++){
            p[3+2*i]=columnDefinitionArrayList.get(i).getColumnName();
            p[3+2*i+1]=columnDefinitionArrayList.get(i).toStringDataTypeAndSpec();
        }
        return memConnect.CreateOriginClass(p);
    }
}
