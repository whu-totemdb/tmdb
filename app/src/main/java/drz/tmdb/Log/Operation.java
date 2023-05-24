package drz.tmdb.Log;

import drz.tmdb.Memory.MemManager;
import drz.tmdb.Memory.Tuple;

//模拟单个操作
public class Operation {
    public MemManager memManager;
    Tuple t;
    public Operation(MemManager memManager,Tuple t){
        this.memManager=memManager;
        this.t=t;
    }
    public void execute(){
        memManager.add(t);
    }

    public Tuple getTuple(){
        return t;
    }
}
