package drz.tmdb.Memory.SystemTable;

import java.io.Serializable;

public class SwitchingTableItem implements Serializable {
    public String attr = "";
    public String deputy = "";
    public String rule = "";

    public SwitchingTableItem(String attr, String deputy, String rule) {
        this.attr = attr;
        this.deputy = deputy;
        this.rule = rule;
    }

    public SwitchingTableItem(){}
}
