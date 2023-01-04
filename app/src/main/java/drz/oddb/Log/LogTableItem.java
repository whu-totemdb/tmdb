package drz.oddb.Log;

public class LogTableItem {
    public int length=0;
    public String str=null;
    public LogTableItem(String s){
        this.str=s;
        this.length=s.length();
    }
    public LogTableItem(){};
}
