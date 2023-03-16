package drz.tmdb.Memory.SystemTable;

import java.io.Serializable;

public class DeputyTableItem implements Serializable {
    public DeputyTableItem(int originid, int deputyid, String[] deputyrule) {
        this.originid = originid;
        this.deputyid = deputyid;
        this.deputyrule = deputyrule;
    }

    public DeputyTableItem() {
    }

    public int originid = 0;            //类id
    public int deputyid = 0;           //代理类id
    public String[] deputyrule = new String[0];    //代理guizedui


}
