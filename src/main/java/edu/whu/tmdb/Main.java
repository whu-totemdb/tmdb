package edu.whu.tmdb;

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.util.DbOperation;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static edu.whu.tmdb.util.DbOperation.*;
import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

public class Main {
    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String sqlCommand;

        // 调试用
        while (true) {
            System.out.print("tmdb> ");
            sqlCommand = reader.readLine().trim();
            if ("exit".equalsIgnoreCase(sqlCommand)) {
                break;
            } else if ("resetdb".equalsIgnoreCase(sqlCommand)) {
                DbOperation.resetDB();
            } else if ("show BiPointerTable".equalsIgnoreCase(sqlCommand)) {
                DbOperation.showBiPointerTable();
            } else if ("show ClassTable".equalsIgnoreCase(sqlCommand)) {
                DbOperation.showClassTable();
            } else if ("show DeputyTable".equalsIgnoreCase(sqlCommand)) {
                DbOperation.showDeputyTable();
            } else if ("show SwitchingTable".equalsIgnoreCase(sqlCommand)) {
                DbOperation.showSwitchingTable();
            } else if (!sqlCommand.isEmpty()) {
                SelectResult result = execute(sqlCommand);
                if (result != null) {
                    DbOperation.printResult(result);
                }
            }
        }

        // execute("show tables;");
        // execute(args[0]);
        // transaction.test();
        // transaction.test2();
        // insertIntoTrajTable();
        // testMapMatching();
        // testEngine();
        // testTorch3();
    }

    public static void insertIntoTrajTable(){
        Transaction transaction = Transaction.getInstance();
        transaction.insertIntoTrajTable();
        transaction.SaveAll();
    }

    public static void testEngine() throws IOException {
        Transaction transaction = Transaction.getInstance();
        transaction.testEngine();
    }

    public static void testMapMatching() {
        Transaction transaction = Transaction.getInstance();
        transaction.testMapMatching();
    }

    public static SelectResult execute(String s)  {
        Transaction transaction = Transaction.getInstance();    // 创建一个事务实例
        SelectResult selectResult = null;
        try {
            // 使用JSqlparser进行sql语句解析，会根据sql类型生成对应的语法树
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
            Statement stmt = CCJSqlParserUtil.parse(byteArrayInputStream);
            selectResult = transaction.query("", -1, stmt);
            if(!stmt.getClass().getSimpleName().toLowerCase().equals("select")){
                transaction.SaveAll();
            }
        }catch (JSQLParserException e) {
            // e.printStackTrace();    // 打印语法错误的堆栈信息
            System.out.println("syntax error");
        }
        return selectResult;
    }

}