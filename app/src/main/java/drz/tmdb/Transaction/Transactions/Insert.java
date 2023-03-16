package drz.tmdb.Transaction.Transactions;

import net.sf.jsqlparser.statement.Statement;

import java.util.ArrayList;

import drz.tmdb.Transaction.Transactions.Exception.TMDBException;

public interface Insert {
    ArrayList<Integer> insert(Statement stmt) throws TMDBException;
}
