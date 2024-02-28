package edu.whu.tmdb.query.operations.Exception;

import java.util.Objects;
import edu.whu.tmdb.query.operations.Exception.ErrorList;

public class TMDBException extends Exception{
    private String name = "";   // 针对表名/属性名不存在的error
    private int id = -1;        // 针对表id/属性id不存在的error
    private int error = -1;     // error类型编号
    private String type = "";   // 针对where表达式中类型不支持的error

    // 使用super方法显示调用父类构造函数
    public TMDBException(int error, String nameOrType) {
        super(nameOrType);
        switch (error) {
            case ErrorList.TYPE_IS_NOT_SUPPORTED:
                this.type = nameOrType;
                break;
            default:
                this.name = nameOrType;
        }
        this.error = error;
    }

    public TMDBException(int error, int id) {
        // super("class with ID: " + id + " doesn't exist!");
        this.id = id;
        this.error = error;
    }

    public TMDBException(int error) {
        super("syntax error");
        this.error = error;
    }

    public TMDBException() {}

    public void printError(){
        switch (error) {
            case ErrorList.TABLE_ALREADY_EXISTS:
                System.out.println("table " + name + " already exists"); break;
            case ErrorList.CLASS_NAME_DOES_NOT_EXIST:
                System.out.println("class named " + name + " does not exist"); break;
            case ErrorList.CLASS_ID_DOES_NOT_EXIST:
                System.out.println("class with ID: " + id + " does not exist"); break;
            case ErrorList.COLUMN_NAME_DOES_NOT_EXIST:
                System.out.println("column named " + name + " does not exist"); break;
            case ErrorList.COLUMN_ID_DOES_NOT_EXIST:
                System.out.println("column with ID: " + id + " does not exist"); break;
            case ErrorList.MISSING_FROM_CLAUSE:
                System.out.println("SELECT SYNTAX ERROR: missing FROM-clause entry"); break;
            case ErrorList.TYPE_IS_NOT_SUPPORTED:
                System.out.println("type: " + type + " is not supported"); break;
            case ErrorList.TYPE_DOES_NOT_MATCH:
                System.out.println("type does not match"); break;
            default:
                System.out.println("ERROR"); break;
        }
    }

}
