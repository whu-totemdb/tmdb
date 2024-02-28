package edu.whu.tmdb.query.operations;

import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import net.sf.jsqlparser.JSQLParserException;

import java.io.IOException;

public interface Select {
    SelectResult select(Object stmt) throws TMDBException, IOException;
}
