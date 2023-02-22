package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.statement.Statement;

public class Drop {
    public boolean drop(Statement statement){
        return execute((net.sf.jsqlparser.statement.drop.Drop) statement);
    }

    public boolean execute(net.sf.jsqlparser.statement.drop.Drop drop){
        String[] p=new String[2];
        p[0]="-1";
        p[1]=drop.getName().getName();
        return new MemConnect().drop(p);
    }
}
