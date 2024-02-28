package edu.whu.tmdb.query.operations;

import net.sf.jsqlparser.statement.Statement;

import edu.whu.tmdb.query.operations.Exception.TMDBException;

public interface Drop {
    boolean drop(Statement statement) throws TMDBException;
}
