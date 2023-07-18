package drz.tmdb.Log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import drz.tmdb.memory.MemManager;
import drz.tmdb.memory.Tuple;

public class Test {

    public static void test1() throws IOException, InterruptedException {
//        LogTableItem[] redo_log;
//        LogManager logManager=new LogManager();
//        logManager.WriteLog("a", (byte) 0,"va");
//        logManager.WriteLog("b", (byte) 1,"va");
//        logManager.setCheckpoint();//设置检查点
//        logManager.WriteLog("c", (byte) 1,"vc");
//        logManager.WriteLog("d", (byte) 0,"vd");

        MemManager memManager = new MemManager();
        List<Tuple> tList = memManager.tupleList.tuplelist;
        int t_num = 0;
        Tuple t1 = new Tuple();
        t1.tupleId = 1;
        t1.tuple = new String[]{"张三", "20", "男", "计算机科学与技术"};
        memManager.add(t1); // 添加日志

        Tuple t2 = new Tuple();
        t2.tupleId = 2;
        t2.tuple = new String[]{"李四", "21", "男", "软件工程"};
        memManager.add(t2); // 添加日志

        Tuple result1 = (Tuple) memManager.search("t" + 1); // ""
        Tuple result2 = (Tuple)  memManager.search("t" + 2); // ""

        t_num = tList.size();
        System.out.print("此时内存中所有tupleId为：");
        for (int i = 0; i < t_num; i++) {
            System.out.print(tList.get(i).tupleId + " ");
        }
        System.out.println();

        memManager.saveMemTableToFile();//刷盘
        result1 = JSON.parseObject((String) memManager.search("t" + 1), Tuple.class); // ""
        result2 = JSON.parseObject((String) memManager.search("t" + 2), Tuple.class); // ""
        t_num = tList.size();
        System.out.print("此时内存中所有tupleId为：");
        for (int i = 0; i < t_num; i++) {
            System.out.print(tList.get(i).tupleId + " ");
        }
        System.out.println();

        Tuple t3 = new Tuple();
        t3.tupleId = 3;
        t3.tuple = new String[]{"小红", "20", "女", "计算机科学与技术"};
        memManager.add(t3);

        Tuple t4 = new Tuple();
        t4.tupleId = 4;
        t4.tuple = new String[]{"王五", "22", "男", "计算机科学与技术"};
        memManager.add(t4);

        Tuple result3 = (Tuple) memManager.search("t" + 3); // ""
        Tuple result4 = (Tuple) memManager.search("t" + 4); // ""

        t_num = tList.size();
        System.out.print("此时内存中所有tupleId为：");
        for (int i = 0; i < t_num; i++) {
            System.out.print(tList.get(i).tupleId + " ");
        }
        System.out.println();

        memManager.tupleList.tuplelist.clear();
        System.out.println("模拟此时系统崩溃了！");

        result3 = JSON.parseObject((String) memManager.search("t" + 3), Tuple.class); // null
        result4 = JSON.parseObject((String) memManager.search("t" + 4), Tuple.class); // null

        t_num = tList.size();
        System.out.print("此时内存中所有tupleId为：");
        for (int i = 0; i < t_num; i++) {
            System.out.print(tList.get(i).tupleId + " ");
        }
        System.out.println();

        memManager.logManager.redo();

        result3 =JSON.parseObject((String) memManager.search("t" + 3), Tuple.class); // ""
        result4 = JSON.parseObject((String) memManager.search("t" + 4), Tuple.class); // ""

        t_num = tList.size();
        System.out.print("此时内存中所有tupleId为：");
        for (int i = 0; i < t_num; i++) {
            System.out.print(tList.get(i).tupleId + " ");
        }
        System.out.println();
    }
}