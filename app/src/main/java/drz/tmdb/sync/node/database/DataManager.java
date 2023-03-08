package drz.tmdb.sync.node.database;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DataManager implements Serializable {

    private ConcurrentLinkedQueue<Action> actionList;

    private final int maxActionNum = 65536;

    private volatile int count;

    private Action[] actions;

    public DataManager() {
        actionList = new ConcurrentLinkedQueue<>();

        actions = new Action[maxActionNum];
        count = 0;
    }

    public boolean isFull(){
        return (count >= maxActionNum);
    }

    public void putAction(Action action){
        synchronized (actionList){
            actionList.add(action);
            actionList.notifyAll();
        }

    }

    public Action getAction(){
        Action head;

        synchronized (actionList){
            if(actionList.isEmpty()){
                try {
                    actionList.wait();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

            head = actionList.poll();
            actionList.notifyAll();

        }

        return head;
    }

}
