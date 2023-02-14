package drz.tmdb.Log;

public class LogTableItem {
    public int logid;//日志记录id
    public Byte op;//0表示插入，1表示删除操作
    public int length;//键和值的长度
    public String key;//键
    public String value;//值
    protected long offset;//日志记录在文件中的偏移量

    public LogTableItem(int logid,Byte op,String key,String value){
        this.logid=logid;
        this.op=op;
        this.length=key.length()+value.length();
        this.key=key;
        this.value=value;
    }
    public LogTableItem(){};
}
