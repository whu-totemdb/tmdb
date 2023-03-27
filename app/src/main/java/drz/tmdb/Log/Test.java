package drz.tmdb.Log;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Test {

    public static void test1() throws IOException {
        LogTableItem[] redo_log;
        LogManager logManager=new LogManager();
        logManager.WriteLog("a", (byte) 0,"va");
        logManager.WriteLog("b", (byte) 1,"va");
        logManager.setCheckpoint();//设置检查点
        logManager.WriteLog("c", (byte) 1,"vc");
        logManager.WriteLog("d", (byte) 0,"vd");

        //加载日志测试
        //logManager.loadLog();

        //redo测试
        //redo_log=logManager.readRedo();
        //System.out.println("id为"+redo_log[2].logid+" key为"+redo_log[2].key);

        //写日志测试
        //raf.seek(0);
        //System.out.println("id为"+raf.readInt()+"op为"+raf.readByte()+"key为"
        //        +raf.readUTF()+"value为"+raf.readUTF()+"offset为"+raf.readLong());
        //System.out.println("id为"+raf.readInt()+"op为"+raf.readByte()+"key为"
        //        +raf.readUTF()+"value为"+raf.readUTF()+"offset为"+raf.readLong());
        //raf.close();

        //初始化测试
        logManager.init();

        //删除部分日志测试
        //logManager.DeleteLog();
        //raf.seek(0);
        //System.out.println("id为"+raf.readInt()+" op为"+raf.readByte()+" key为"
         //      +raf.readUTF()+" value为"+raf.readUTF()+" offset为"+raf.readLong());

//        //索引部分测试
//        while (LogManager.iterator.hasNext()) {
//            Map.Entry<String, List<Integer>> entry = LogManager.iterator.next();
//            System.out.println(entry.getKey());
//            List<Integer> listmap=new ArrayList<Integer>();
//            listmap=entry.getValue();
//            for(int i=0;i<listmap.size();i++){
//                System.out.println(listmap.get(i).toString());
//            }
//        }

//        //查询日志记录测试
//        LogTableItem log= logManager.searchLog(3);
//        System.out.println(log.key);
//        return;


    }

    /**
    private void PowerFailure(){
        Calendar calendar=Calendar.getInstance();
        int second=calendar.get(Calendar.SECOND);
        if(second<30){
            throw new RuntimeException("断电宕机...");
        }
    }
     **/

    //public static void main(String[ ] args) throws IOException {

   // }

}