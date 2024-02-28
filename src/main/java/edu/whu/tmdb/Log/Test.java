package edu.whu.tmdb.Log;


import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.Tuple;

public class Test {

//    public static void test1() {
////        LogTableItem[] redo_log;
////        LogManager logManager=new LogManager();
////        logManager.WriteLog("a", (byte) 0,"va");
////        logManager.WriteLog("b", (byte) 1,"va");
////        logManager.setCheckpoint();//设置检查点
////        logManager.WriteLog("c", (byte) 1,"vc");
////        logManager.WriteLog("d", (byte) 0,"vd");
//
//        MemManager memManager = new MemManager();
//        List<Tuple> tList = memManager.tupleList.tuplelist;
//        int t_num=0;
//        Tuple t1 = new Tuple();
//        t1.tupleId = 1;
//        t1.tuple = new String[]{"张三", "20","男","计算机科学与技术"};
//        memManager.add(t1); // 添加日志
//
//        Tuple t2 = new Tuple();
//        t2.tupleId = 2;
//        t2.tuple = new String[]{"李四", "21","男","软件工程"};
//        memManager.add(t2); // 添加日志
//
//        Tuple result1 = memManager.search("" + 1); // ""
//        Tuple result2 = memManager.search("" + 2); // ""
//
//        t_num=tList.size();
//        System.out.print("此时内存中所有tupleId为：");
//        for(int i=0;i<t_num;i++){
//            System.out.print(tList.get(i).tupleId+" ");
//        }
//        System.out.println();
//
//        memManager.saveMemTableToFile();//刷盘
//        result1 = memManager.search("" + 1); // ""
//        result2 = memManager.search("" + 2); // ""
//        t_num=tList.size();
//        System.out.print("此时内存中所有tupleId为：");
//        for(int i=0;i<t_num;i++){
//            System.out.print(tList.get(i).tupleId+" ");
//        }
//        System.out.println();
//
//        Tuple t3 = new Tuple();
//        t3.tupleId = 3;
//        t3.tuple = new String[]{"小红", "20","女","计算机科学与技术"};
//        memManager.add(t3);
//
//        Tuple t4 = new Tuple();
//        t4.tupleId = 4;
//        t4.tuple = new String[]{"王五", "22","男","计算机科学与技术"};
//        memManager.add(t4);
//
//        Tuple result3 = memManager.search("" + 3); // ""
//        Tuple result4 = memManager.search("" + 4); // ""
//
//        t_num=tList.size();
//        System.out.print("此时内存中所有tupleId为：");
//        for(int i=0;i<t_num;i++){
//            System.out.print(tList.get(i).tupleId+" ");
//        }
//        System.out.println();
//
//        memManager.tupleList.tuplelist.clear();
//        System.out.println("模拟此时系统崩溃了！");
//
//        result3 = memManager.search("" + 3); // null
//        result4 = memManager.search("" + 4); // null
//
//        t_num=tList.size();
//        System.out.print("此时内存中所有tupleId为：");
//        for(int i=0;i<t_num;i++){
//            System.out.print(tList.get(i).tupleId+" ");
//        }
//        System.out.println();
//
//        memManager.logManager.redo();
//
//        result3 = memManager.search("" + 3); // ""
//        result4 = memManager.search("" + 4); // ""
//
//        t_num=tList.size();
//        System.out.print("此时内存中所有tupleId为：");
//        for(int i=0;i<t_num;i++){
//            System.out.print(tList.get(i).tupleId+" ");
//        }
//        System.out.println();
//
//        //加载日志测试
//        //logManager.loadLog();
//
//        //redo测试
//        //redo_log=logManager.readRedo();
//        //System.out.println("id为"+redo_log[2].logid+" key为"+redo_log[2].key);
//
//        //写日志测试
//        //raf.seek(0);
//        //System.out.println("id为"+raf.readInt()+"op为"+raf.readByte()+"key为"
//        //        +raf.readUTF()+"value为"+raf.readUTF()+"offset为"+raf.readLong());
//        //System.out.println("id为"+raf.readInt()+"op为"+raf.readByte()+"key为"
//        //        +raf.readUTF()+"value为"+raf.readUTF()+"offset为"+raf.readLong());
//        //raf.close();
//
//        //初始化测试
//        memManager.logManager.init();
//        System.out.println("此时日志文件长度为"+memManager.logManager.logFile.length());
//        //删除部分日志测试
//        //logManager.DeleteLog();
//        //raf.seek(0);
//        //System.out.println("id为"+raf.readInt()+" op为"+raf.readByte()+" key为"
//         //      +raf.readUTF()+" value为"+raf.readUTF()+" offset为"+raf.readLong());
//
////        //索引部分测试
////        while (LogManager.iterator.hasNext()) {
////            Map.Entry<String, List<Integer>> entry = LogManager.iterator.next();
////            System.out.println(entry.getKey());
////            List<Integer> listmap=new ArrayList<Integer>();
////            listmap=entry.getValue();
////            for(int i=0;i<listmap.size();i++){
////                System.out.println(listmap.get(i).toString());
////            }
////        }
//
////        //查询日志记录测试
////        LogTableItem log= logManager.searchLog(3);
////        System.out.println(log.key);
////        return;
//
//
//    }

    /**
    private void PowerFailure(){
        Calendar calendar=Calendar.getInstance();
        int second=calendar.get(Calendar.SECOND);
        if(second<30){
            throw new RuntimeException("断电宕机...");
        }
    }
     **/

    //public static void main(String[ ] args) {

   // }

}