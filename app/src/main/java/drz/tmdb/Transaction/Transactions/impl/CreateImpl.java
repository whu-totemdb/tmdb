package drz.tmdb.Transaction.Transactions.impl;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.ArrayList;

import drz.tmdb.memory.SystemTable.ClassTableItem;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.Create;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;

public class CreateImpl implements Create {
    private MemConnect memConnect=new MemConnect();
    public CreateImpl(){}

    public CreateImpl(MemConnect memConnect) {
        this.memConnect = memConnect;
    }

    public boolean create(Statement stmt) throws TMDBException {
        return execute((CreateTable) stmt);
    }

    //CREATE CLASS dZ123 (nB1 int,nB2 char) ;
    //1,2,dZ123,nB1,int,nB2,char
    public boolean execute(CreateTable stmt) throws TMDBException {
        //获取新定义class具体元素
        ArrayList<ColumnDefinition> columnDefinitionArrayList= (ArrayList<ColumnDefinition>) stmt.getColumnDefinitions();
        //以下操作创建memConnect中的create origin class需要的String 数组，传入其中进行实际的创建工作。
        String[] p=new String[columnDefinitionArrayList.size()*2+3];
        p[0]="";
        p[1]=""+columnDefinitionArrayList.size();
        p[2]=stmt.getTable().toString();
        for(int i=0;i<columnDefinitionArrayList.size();i++){
            p[3+2*i]=columnDefinitionArrayList.get(i).getColumnName();
            p[3+2*i+1]=columnDefinitionArrayList.get(i).toStringDataTypeAndSpec();
        }
        return this.CreateOriginClass(p);
    }

    public boolean CreateOriginClass(String[] p) throws TMDBException {
        String classname = p[2];
        int count = Integer.parseInt(p[1]);
        memConnect.getClasst().maxid++;
        int classid = memConnect.getClasst().maxid;
        for(ClassTableItem item : memConnect.getClasst().classTable){
            if(item.classname.equals(classname)){
                throw new TMDBException(classname+"已经存在！");
            }
        }
        for (int i = 0; i < count; i++) {
            memConnect.getClasst().classTable.add(new ClassTableItem(classname, classid, count,i,p[2 * i + 3], p[2 * i + 4],"ori",""));
        }
//        this.SaveAll();
        return true;
    }
}
