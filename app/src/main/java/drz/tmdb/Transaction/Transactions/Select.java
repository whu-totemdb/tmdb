package drz.tmdb.Transaction.Transactions;

import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.utils.SelectResult;

public interface Select {
    SelectResult select(Object stmt) throws TMDBException;
}
