package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.statement.Statement;

import drz.tmdb.Transaction.Transactions.Exception.TMDBException;

public interface CreateDeputyClass {
    boolean createDeputyClass(Statement stmt) throws TMDBException;
}
