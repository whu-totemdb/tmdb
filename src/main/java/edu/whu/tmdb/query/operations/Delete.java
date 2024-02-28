package edu.whu.tmdb.query.operations;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;

import edu.whu.tmdb.query.operations.Exception.TMDBException;

public interface Delete {
    void delete(Statement statement) throws JSQLParserException, TMDBException, IOException;
}
