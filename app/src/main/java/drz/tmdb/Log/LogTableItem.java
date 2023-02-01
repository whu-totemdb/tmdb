package drz.tmdb.Log;

public class LogTableItem {
    public int op;//0表示插入，1表示删除操作
    public int status;//0表示success，1表示fail
    public String key;//键
    public String value;//值

    public LogTableItem(int op,int status,String key,String value){
        this.op=op;
        this.status=status;
        this.key=key;
        this.value=value;
    }
    public LogTableItem(){};
}
