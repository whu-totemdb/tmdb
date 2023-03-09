package drz.tmdb.Transaction.Transactions.impl;

import net.sf.jsqlparser.statement.Statement;

import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.Drop;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;

public class DropImpl implements Drop {
    public boolean drop(Statement statement) throws TMDBException {
        return execute((net.sf.jsqlparser.statement.drop.Drop) statement);
    }

    public boolean execute(net.sf.jsqlparser.statement.drop.Drop drop) throws TMDBException {
        String[] p=new String[2];
        p[0]="-1";
        //这里是删除表的表名
        p[1]=drop.getName().getName();
        return new MemConnect().drop(p);
    }
}
