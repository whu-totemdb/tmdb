package edu.whu.tmdb.query;


import edu.whu.tmdb.query.operations.impl.*;
import edu.whu.tmdb.query.operations.torch.TorchConnect;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import edu.whu.tmdb.Log.LogManager;
import edu.whu.tmdb.query.operations.Create;
import edu.whu.tmdb.query.operations.CreateDeputyClass;
import edu.whu.tmdb.query.operations.Delete;
import edu.whu.tmdb.query.operations.Drop;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Insert;
import edu.whu.tmdb.query.operations.Select;
import edu.whu.tmdb.query.operations.Update;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.level.LevelManager;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Transaction {

    private static Logger logger = LoggerFactory.getLogger(Transaction.class);
    public MemManager mem;
    public LevelManager levelManager;
    public LogManager log;
    private MemConnect memConnect;

    // 1. 私有静态变量，用于保存MemConnect的单一实例
    private static volatile Transaction instance = null;        // volatile关键字使线程对 instance 的修改对其他线程立刻可见

    // 2. 提供一个全局访问点
    public static Transaction getInstance(){
        // 双重检查锁定模式
        try {
            if (instance == null) { // 第一次检查
                synchronized (Transaction.class) {
                    if (instance == null) { // 第二次检查
                        instance = new Transaction();
                    }
                }
            }
            return instance;
        }catch (TMDBException e){
            // logger.warn(e.getMessage());
            e.printError();
        }catch (JSQLParserException e){
            logger.warn(e.getMessage());
        }catch (IOException e){
            logger.error(e.getMessage());
        }
        return instance;
    }

    private Transaction() throws IOException, JSQLParserException, TMDBException {
        // 防止通过反射创建多个实例
        if (instance != null) {
            throw new RuntimeException("Use getInstance() method to get the single instance of this class.");
        }
        this.mem = MemManager.getInstance();
        this.levelManager = MemManager.levelManager;
        this.memConnect = MemConnect.getInstance(mem);
    }


    public void clear() {
//        File classtab=new File("/data/data/edu.whu.tmdb/transaction/classtable");
//        classtab.delete();
        File objtab=new File("/data/data/edu.whu.tmdb/transaction/objecttable");
        objtab.delete();
    }

    public void SaveAll( ) { memConnect.SaveAll(); }

    public void reload() { memConnect.reload(); }

    public void Test(){
        TupleList tpl = new TupleList();
        Tuple t1 = new Tuple();
        t1.tupleSize = 5;
        t1.tuple = new Object[t1.tupleSize];
        t1.tuple[0] = "a";
        t1.tuple[1] = 1;
        t1.tuple[2] = "b";
        t1.tuple[3] = 3;
        t1.tuple[4] = "e";
        Tuple t2 = new Tuple();
        t2.tupleSize = 5;
        t2.tuple = new Object[t2.tupleSize];
        t2.tuple[0] = "d";
        t2.tuple[1] = 2;
        t2.tuple[2] = "e";
        t2.tuple[3] = 2;
        t2.tuple[4] = "v";

        tpl.addTuple(t1);
        tpl.addTuple(t2);
        String[] attrname = {"attr2","attr1","attr3","attr5","attr4"};
        int[] attrid = {1,0,2,4,3};
        String[]attrtype = {"int","char","char","char","int"};

    }

    public SelectResult query(String s) throws JSQLParserException {
        // 使用JSqlparser进行sql语句解析，会根据sql类型生成对应的语法树
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
        Statement stmt= CCJSqlParserUtil.parse(byteArrayInputStream);

        return this.query("", -1, stmt);
    }

    public SelectResult query(Statement s) {
        return this.query("", -1, s);
    }

    public SelectResult query(String k, int op, Statement stmt) {
        SelectResult selectResult = null;
        try {
            // 获取生成语法树的类型，用于进一步判断
            String sqlType = stmt.getClass().getSimpleName();

            switch (sqlType) {
                case "CreateTable":
//                    log.WrteLog(s);
                    Create create = new CreateImpl();
                    create.create(stmt);
                    break;
                case "CreateDeputyClass":
//                    log.WriteLog(id,k,op,s);
                    CreateDeputyClass createDeputyClass = new CreateDeputyClassImpl();
                    createDeputyClass.createDeputyClass(stmt);
                    break;
                case "CreateTJoinDeputyClass":
                    // log.WriteLog(id,k,op,s);
                    CreateTJoinDeputyClassImpl createTJoinDeputyClass = new CreateTJoinDeputyClassImpl();
                    createTJoinDeputyClass.createTJoinDeputyClass(stmt);
                    break;
                case "Drop":
//                    log.WriteLog(id,k,op,s);
                    Drop drop = new DropImpl();
                    drop.drop(stmt);
                    break;
                case "Insert":
//                    log.WriteLog(id,k,op,s);
                    Insert insert = new InsertImpl();
                    insert.insert(stmt);
                    break;
                case "Delete":
 //                   log.WriteLog(id,k,op,s);
                    Delete delete = new DeleteImpl();
                    delete.delete(stmt);
                    break;
                case "Select":
                    Select select = new SelectImpl();
                    selectResult = select.select(stmt);
                    break;
                case "Update":
 //                   log.WriteLog(id,k,op,s);
                    Update update = new UpdateImpl();
                    update.update(stmt);
                    break;
                default:
                    break;
            }
        } catch (JSQLParserException e) {
            logger.warn(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        } catch (TMDBException e) {
            e.printError();
        }

        return selectResult;
    }

    public void testMapMatching() {
        TorchConnect torchConnect = new TorchConnect(memConnect,"Torch_Porto_test");
//        torchConnect.insert("data/res/raw/porto_raw_trajectory.txt");
//        this.SaveAll();
        torchConnect.mapMatching();
    }

    public void testEngine() throws IOException {
        TorchConnect torchConnect = new TorchConnect(memConnect,"Torch_Porto_test");
        torchConnect.initEngine();
        torchConnect.test ();
    }

    public void insertIntoTrajTable() {
        TorchConnect torchConnect = new TorchConnect(memConnect,"Torch_Porto_test");
        torchConnect.insert("data/res/raw/porto_raw_trajectory.txt");
    }


}

