package drz.oddb.Log;

import org.json.JSONObject;

public class LogTableItem {
    public String key;
    public String value;
    public int op;//日志记录操作类型：op为1表示insert，op为2表示delete
    public int status;//日志记录状态，0表示未完成，1表示已完成
    public LogTableItem(String k,String v,int op,int status){
        this.key=k;
        this.value=v;
        this.op=op;
        this.status=status;
    }
    public LogTableItem(){};
}
