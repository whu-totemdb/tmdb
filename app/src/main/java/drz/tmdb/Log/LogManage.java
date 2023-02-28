package drz.tmdb.Log; /**package drz.tmdb.Log;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import drz.tmdb.Level.Constant;

public class LogManage {
    final private int attrstringlen=8; //属性最大字符串长度为8Byte
    final private int MAXSIZE=20*1024;//一个log文件最多写20kb日志数据
    private int checkpoint=-1;//检查点
    public int logid;    //创建LogManager时LogTable块id
    private int fileID;
    public LogTableItem logTableItem = new LogTableItem();   //存放执行层创建LogManager时写入的日志

    //构造方法
    public LogManage(){
        logid=0;
        fileID=0;
    }

    //得到检查点号
    /**private int GetCheck(){
        int cpid = trans.mem.loadCheck();
        return cpid;
    }


    //日志存入SStable
    public void writeLogItemToSSTable(LogTableItem log){
        try{
            File logFile_in;
            File curfile = new File(Constant.DATABASE_DIR + fileID);
            if(!curfile.exists()){
                    curfile.createNewFile();
            }
            if(curfile.length()+10+log.v_length>=MAXSIZE){
                fileID++;
                logFile_in = new File(Constant.DATABASE_DIR + fileID);
                logFile_in.createNewFile();
            }else{
                logFile_in = curfile;
            }

            // 写一条log
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(logFile_in, true));

            byte[] ID=int2Bytes(log.logid,2);
            output.write(ID,0,2);
            byte[] operation=int2Bytes(log.op,1);
            output.write(operation,0,1);
            byte[] sta=int2Bytes(log.status,1);
            output.write(sta,0,1);
            byte[] v_len=int2Bytes(log.v_length,4);
            output.write(v_len,0,4);
            byte[] Key=KEY_TO_BYTES(log.key);
            output.write(Key,0,2);
            byte[] Value=str2Bytes(log.value);
            output.write(Value,0,Value.length);
            output.flush();
            output.close();

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    //编码int为byte
    private byte[] int2Bytes(int value, int len){
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[len - i - 1] = (byte)(value >> 8 * i);
        }
        return b;
    }
    //编码key为byte
    public static final byte[] KEY_TO_BYTES(String key){
        byte[] ret = new byte[Constant.MAX_KEY_LENGTH];
        byte[] temp = key.getBytes();
        if(temp.length <= Constant.MAX_KEY_LENGTH){
            for(int i=0;i<temp.length;i++){
                ret[i]=temp[i];
            }
            for(int i=temp.length;i<Constant.MAX_KEY_LENGTH;i++){
                // 不足的地方补全0
                ret[i]=(byte)32;
            }
        }
        return ret;
    }
    //编码字符串为byte
    private byte[] str2Bytes(String s){
        byte[] ret=new byte[attrstringlen];
        byte[] temp=s.getBytes();
        if(temp.length>=attrstringlen){
            for(int i=0;i<attrstringlen;i++){
                ret[i]=temp[i];
            }
            return ret;
        }else{
            for(int i=0;i<temp.length;i++){
                ret[i]=temp[i];
            }
            for(int i=temp.length;i<attrstringlen;i++){
                ret[i]=(byte)32;
            }
            return ret;
        }
    }

    //解码byte为int
    private int bytes2Int(byte[] b, int start, int len) {
        int sum = 0;
        int end = start + len;
        for (int i = start; i < end; i++) {
            int n = b[i]& 0xff;
            n <<= (--len) * 8;
            sum += n;
        }
        return sum;
    }

    // 解码byte[]为key
    public static final String BYTES_TO_KEY(byte[] b){
        String s;
        int k=0;
        for(int i=0;i<Constant.MAX_KEY_LENGTH;i++){
            if(b[i]!=32){
                k++;
            }else{
                break;
            }
        }
        s=new String(b,0,k);
        return s;
    }

    //写日志
    public boolean WriteLog(String k,int op,String s){
        LogTableItem LogItem = new LogTableItem(logid,op,0,k,s);     //把语句传入logItem，这个时候都是未完成
        writeLogItemToSSTable(LogItem);
        if(lognum<MAXSIZE){  //List写得下
            LogT.logTable.add(LogItem);
            if(lognum == (MAXSIZE-1)){
                writeLogItemToSSTable(LogT);
                while(!trans.mem.flush());
                while(!setLogCheck(LogT.logID));
                trans.mem.setCheckPoint(LogT.logID);
                DeleteLog();
            }
        }else{
            writeLogItemToSSTable(LogT);
            while(!trans.mem.flush());
            while(!setLogCheck(LogT.logID));
            trans.mem.setCheckPoint(LogT.logID);
            DeleteLog();
            init(); //新建一个日志块
            LogT.logTable.add(LogItem);
            LogT.logID=logid+1;
        }

        logid++;//logid+1
        return true;
    }

    //设置日志块检查点为1
    /**
    public boolean setLogCheck(int logid){
        LogTable l;
        if((l=this.readLog(logid))!=null){
            l.check=1;
            this.writeLogItemToSSTable(l);
            return true;
        }else{
            return false;
        }
    }


    //读取日志块
    public  LogTableItem readLog(int logID){
        LogTableItem temp;
        File logfile_out = new File(Constant.DATABASE_DIR + logID);
        if(!logfile_out.exists()){
            return null;
        }else{
            try {
                FileInputStream input=new FileInputStream(logfile_out);
                temp=new LogTableItem();
                byte[] x=new byte[2];
                input.read(x,0,2);
                temp.logid=bytes2Int(x,0,2);

                temp=new LogTableItem();
                byte[] y=new byte[1];
                temp.op=bytes2Int(y,0,1);
                byte[] z=new byte[1];
                input.read(z,0,1);
                temp.status=bytes2Int(z,0,1);
                byte[] p=new byte[2];
                input.read(p,0,2);
                temp.key=BYTES_TO_KEY(p);
                byte[] q=new byte[temp.value.length()];
                input.read(q,0,temp.value.length());
                temp.value=new String(q,0,temp.value.length());

                input.close();
                temp.logid=logID;
                return temp;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }



    //redo日志
    public LogTable GetReDo(){
        LogTable ret = null;
        checkpoint = GetCheck();    //得到检查点id
        ret = readLog(checkpoint+1);   //加载可能redo的日志

        if(ret != null){
            int lognum = ret.logTable.size();    //有几条语句需要redo
            for(int i=0;i<lognum;i++){
                LogTableItem temp = ret.logTable.get(i);
                ret.logTable.add(temp);
            }
            ret.logID=checkpoint+1; //执行层写也可以，不可以写两次
            return ret;
        }
        return ret;
    }


    //删除磁盘上检查点之前的文件
    public void DeleteLog(){
        File dir = new File(Constant.DATABASE_DIR);
        File[] files = dir.listFiles();
        if(files!=null && files.length > 0) {
            for (File f : files) {
                String filename=f.getName();
                int logid=Integer.parseInt(filename);
                if(logid<checkpoint){
                    f.delete();
                }

            }
        }

    }

}
**/
