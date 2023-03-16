package drz.tmdb.Transaction.Transactions.impl;

import net.sf.jsqlparser.statement.Statement;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import drz.tmdb.Memory.SystemTable.BiPointerTableItem;
import drz.tmdb.Memory.SystemTable.ClassTableItem;
import drz.tmdb.Memory.SystemTable.DeputyTableItem;
import drz.tmdb.Memory.SystemTable.ObjectTableItem;
import drz.tmdb.Memory.SystemTable.SwitchingTableItem;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.Drop;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;

public class DropImpl implements Drop {
    private MemConnect memConnect;

    public DropImpl(MemConnect memConnect) {
        this.memConnect = memConnect;
    }

    public DropImpl() {
    }
    public boolean drop(Statement statement) throws TMDBException {
        return execute((net.sf.jsqlparser.statement.drop.Drop) statement);
    }

    public boolean execute(net.sf.jsqlparser.statement.drop.Drop drop) throws TMDBException {
        String[] p=new String[2];
        p[0]="-1";
        //这里是删除表的表名
        p[1]=drop.getName().getName();
        return drop(p);
    }

    //DROP CLASS asd;
    //3,asd
    public boolean drop(String[]p) throws TMDBException {
        List<DeputyTableItem> dti;
        dti = Drop1(p);
        for(DeputyTableItem item:dti){
            memConnect.getDeputyt().deputyTable.remove(item);
        }
        return  true;
    }

    private List<DeputyTableItem> Drop1(String[] p) throws TMDBException {
        String classname = p[1];
        int classid = memConnect.getClassId(p[1]);
        //找到classid顺便 清除类表和switch表

        for (Iterator it1 = memConnect.getClasst().classTable.iterator(); it1.hasNext();) {
            ClassTableItem item =(ClassTableItem) it1.next();
            if (item.classname.equals(classname) ){
                classid = item.classid;
                for(Iterator it = memConnect.getSwitchingT().switchingTable.iterator(); it.hasNext();) {
                    SwitchingTableItem item2 =(SwitchingTableItem) it.next();
                    if (item2.attr.equals( item.attrname)||item2.deputy .equals( item.attrname)){
                        it.remove();
                    }
                }
                it1.remove();
            }
        }
        //清元组表同时清了bi
        MemConnect.OandB ob2 = new MemConnect.OandB();
        DeleteImpl delete=new DeleteImpl(memConnect);
        for(ObjectTableItem item1: memConnect.getTopt().objectTable){
            if(item1.classid == classid){
                MemConnect.OandB ob = delete.DeletebyID(item1.tupleid);
                for(ObjectTableItem obj:ob.o){
                    ob2.o.add(obj);
                }
                for(BiPointerTableItem bip:ob.b){
                    ob2.b.add(bip);
                }
            }
        }
        for(ObjectTableItem obj:ob2.o){
            memConnect.getTopt().objectTable.remove(obj);
        }
        for(BiPointerTableItem bip:ob2.b) {
            memConnect.getBiPointerT().biPointerTable.remove(bip);
        }

        //清deputy
        List<DeputyTableItem> dti = new ArrayList<>();
        for(DeputyTableItem item3: memConnect.getDeputyt().deputyTable){
            if(item3.deputyid == classid){
                if(!dti.contains(item3))
                    dti.add(item3);
            }
            if(item3.originid == classid){
                //删除代理类
                String[]s = p.clone();
                List<String> sname = new ArrayList<>();
                for(ClassTableItem item5: memConnect.getClasst().classTable) {
                    if (item5.classid == item3.deputyid) {
                        sname.add(item5.classname);
                    }
                }
                for(String item4: sname){

                    s[1] = item4;
                    List<DeputyTableItem> dti2 = Drop1(s);
                    for(DeputyTableItem item8:dti2){
                        if(!dti.contains(item8))
                            dti.add(item8);
                    }

                }
                if(!dti.contains(item3))
                    dti.add(item3);
            }
        }
        return dti;

    }
}
