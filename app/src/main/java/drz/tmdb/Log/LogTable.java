package drz.tmdb.Log;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LogTable implements Serializable {
    public int check=0;
    public int logID=0;
    public List<LogTableItem> logTable=new ArrayList<>();
}
