package edu.whu.tmdb.query.operations;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;
import java.util.ArrayList;

import edu.whu.tmdb.query.operations.Exception.TMDBException;

public interface Update {
    void update(Statement stmt) throws JSQLParserException, TMDBException, IOException;
}
