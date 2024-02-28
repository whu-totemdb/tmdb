package edu.whu.tmdb.query.operations.Exception;
/**
 *  TMDB错误处理类，以下为error类型编码
 *  error code  | 错误类型
 *      0       | table *** already exists
 *      1       | class named *** does not exist
 *      2       | class with id *** does not exist
 *      3       | column named *** does not exist
 *      4       | column with id *** does not exist
 *      5       | SELECT SYNTAX ERROR: missing FROM-clause entry
 *      6       | type *** is not supported
 *      6       | type does not match
 */
public class ErrorList {
    public static final int TABLE_ALREADY_EXISTS        = 0;
    public static final int CLASS_NAME_DOES_NOT_EXIST   = 1;
    public static final int CLASS_ID_DOES_NOT_EXIST     = 2;
    public static final int COLUMN_NAME_DOES_NOT_EXIST  = 3;
    public static final int COLUMN_ID_DOES_NOT_EXIST    = 4;
    public static final int MISSING_FROM_CLAUSE         = 5;
    public static final int TYPE_IS_NOT_SUPPORTED       = 6;
    public static final int TYPE_DOES_NOT_MATCH         = 7;
}
