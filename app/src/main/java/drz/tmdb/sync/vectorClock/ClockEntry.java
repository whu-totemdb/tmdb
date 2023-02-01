package drz.tmdb.sync.vectorClock;

import java.io.Serializable;

public class ClockEntry implements Cloneable,Serializable {

    private final static long ride = 1;
    private String nodeID;

    private long version;


    public ClockEntry(){
        this.nodeID = null;
        this.version = -1;
    }

    public ClockEntry(String nodeID, long version) {

        if(version < 1){
            throw new IllegalArgumentException("版本号小于1，为非法输入");
        }

        this.nodeID = nodeID;
        this.version = version;
    }

    public String getNodeID() {
        return nodeID;
    }

    public void setNodeID(String nodeID) {
        this.nodeID = nodeID;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        if(version < 1){
            throw new IllegalArgumentException("版本号小于1，为非法输入");
        }
        this.version = version;
    }

    public ClockEntry clone(){
        try{
            return (ClockEntry) super.clone();
        }catch (CloneNotSupportedException e){
            throw new RuntimeException(e);
        }
    }

    public void increment(){
        setVersion(version + ride);
    }

    //判断两个ClockEntry对象是否相同
    public boolean equals(Object o){
        //同一地址
        if(this == o){
            return true;
        }

        if(o == null){
            return  false;
        }

        //不同地址的ClockEntry对象
        if(o.getClass().equals(ClockEntry.class)){
            ClockEntry clockEntry = (ClockEntry) o;
            if(clockEntry.getNodeID().equals(getNodeID()) && clockEntry.getVersion() == getVersion()){
                return true;
            }else {
                return false;
            }
        }
        else {
            return false;
        }
    }

    public String toString(){
        String str = nodeID + "---" + version;
        return str;
    }

    //验证向量时钟是否合法
    public void validate(){
        if(version < 1){
            throw new RuntimeException("当前时钟实体的版本号非法");
        }
    }
}
