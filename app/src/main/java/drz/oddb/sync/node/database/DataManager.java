package drz.oddb.sync.node.database;

import java.util.concurrent.ConcurrentLinkedQueue;

public class DataManager {

    private ConcurrentLinkedQueue<Action> actionList;//更新操作对应的Action对象

    private ConcurrentLinkedQueue<Action> oldActionList;//更新操作在更新数据前将旧数据封装为Action对象存储在这里

    public DataManager() {

        actionList = new ConcurrentLinkedQueue<>();
        oldActionList = new ConcurrentLinkedQueue<>();
    }


    public void putAction(Action action){
        actionList.add(action);
    }

    public void putOldAction(Action action){
        oldActionList.add(action);
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

    public Action getOldAction(){
        Action head;

        synchronized (oldActionList){
            if(oldActionList.isEmpty()){
                try {
                    oldActionList.wait();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

            head = oldActionList.poll();
            oldActionList.notifyAll();
        }

        return head;

    }
}
