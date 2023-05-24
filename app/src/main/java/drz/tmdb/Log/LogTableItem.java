package drz.tmdb.Log;

import androidx.annotation.NonNull;

public class LogTableItem {
    public int logid;//日志记录id
    public int txn_id;//日志对应的事务id
    protected long offset;//日志记录在文件中的偏移量

    public LogTableItem(int logid,int txn_id){
        this.logid=logid;
        this.txn_id=txn_id;
    }

    public LogTableItem(){
    }

    public int getTxn_id(){
        return txn_id;
    }
    public int getLogID(){
        return logid;
    }


    // BeginRecord子类
    public static class BeginRecord extends LogTableItem {
        public BeginRecord(int logid, int txn_id) {
            super(logid,txn_id);
        }
        @NonNull
        @Override
        public String toString(){
            return "id为"+ this.logid+" 事务id为"+ this.txn_id+" offset为"+ this.offset;
        }
    }

    // CommitRecord子类
    public static class CommitRecord extends LogTableItem {
        public CommitRecord(int logid, int txn_id) {
            super(logid,txn_id);
        }
        @NonNull
        @Override
        public String toString(){
            return "id为"+ this.logid+" 事务id为"+ this.txn_id+" offset为"+ this.offset;
        }
    }

    // EndRecord子类
    public static class EndRecord extends LogTableItem {
        public EndRecord(int logid, int txn_id) {
            super(logid,txn_id);
        }
        @NonNull
        @Override
        public String toString(){
            return "id为"+ this.logid+" 事务id为"+ this.txn_id+" offset为"+ this.offset;
        }
    }

    public static class LogRecord extends LogTableItem{
        public Byte op;//0表示插入，1表示删除操作
        public String key;//键
        public String value;//值
        public LogRecord(int logid,int txn_id,Byte op,String key,String value){
            super(logid,txn_id);
            this.op=op;
            this.key=key;
            this.value=value;
        }

        @NonNull
        @Override
        public String toString(){
            return "id为"+ this.logid+" 事务id为"+ this.txn_id+" op为"+ this.op+" key为"
                    + this.key + " value为"+ this.value +" offset为"+ this.offset;
        }
    }
}


