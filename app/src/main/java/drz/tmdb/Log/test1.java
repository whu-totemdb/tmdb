package drz.tmdb.Log;


import static drz.tmdb.Log.LogManager.raf;

import java.io.IOException;
import java.util.Calendar;

import drz.tmdb.Transaction.TransAction;

public class test1 {

    public static void main(String[ ] args) throws IOException {
        LogTableItem[] redo_log;
        LogManager logManager=new LogManager();
        logManager.WriteLog("a", (byte) 0,"va");
        logManager.WriteLog("b", (byte) 1,"vb");
        logManager.setCheckpoint();//设置检查点
        logManager.WriteLog("c", (byte) 1,"vc");
        logManager.WriteLog("d", (byte) 0,"vd");
        logManager.WriteLog("e", (byte) 0,"ve");

        //加载日志测试
        logManager.loadLog();

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
        //logManager.init();

        //删除部分日志测试
        //logManager.DeleteLog();
        //raf.seek(0);
        //System.out.println("id为"+raf.readInt()+" op为"+raf.readByte()+" key为"
         //      +raf.readUTF()+" value为"+raf.readUTF()+" offset为"+raf.readLong());


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