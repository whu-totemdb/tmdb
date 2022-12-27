package drz.oddb.sync.node;

import drz.oddb.sync.config.GossipConfig;
import drz.oddb.sync.network.GossipController;
import drz.oddb.sync.network.GossipRequest;
import drz.oddb.sync.node.database.Action;
import drz.oddb.sync.node.database.OperationType;
import drz.oddb.sync.share.SendInfo;
import drz.oddb.sync.timeTest.TimeTest;
import drz.oddb.sync.vectorClock.VectorClock;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Node implements Serializable {
    //private final InetSocketAddress socketAddress;//节点的IP套接字（IP地址+端口号）

    private final InetAddress IPAddress;//IP地址

    //private long version = 0;//节点版本号
    private static int requestID = 0;

    private static int maxRequestID = 65536;

    private final int receivePort;//节点的接收端口

    private LocalDateTime lastUpdateTime = null;//节点最新一次更新的时间

    private volatile boolean failed =false;//节点是否故障

    private GossipConfig gossipConfig;

    private ConcurrentHashMap<Long, VectorClock> vectorClockMap;

    private ConcurrentLinkedQueue<Long> syncQueue;

    private ConcurrentHashMap<InetSocketAddress,Boolean> nodeStateTable = new ConcurrentHashMap<>();//节点状态表

    private GossipController gossipController;

    //private byte[] data;



    public Node(InetAddress IPAddress, int receivePort, GossipConfig gossipConfig) {
        //this.socketAddress = socketAddress;
        //this.version = version;
        this.IPAddress = IPAddress;
        this.receivePort = receivePort;
        this.gossipConfig = gossipConfig;



        vectorClockMap = new ConcurrentHashMap<>();
        syncQueue = new ConcurrentLinkedQueue<>();

        initialVectorClockMap();//初始化应用数据库

        InetSocketAddress socketAddress = new InetSocketAddress(IPAddress,receivePort);

        if(failed)
            nodeStateTable.putIfAbsent(socketAddress,false);
        else
            nodeStateTable.putIfAbsent(socketAddress,true);


        gossipController = new GossipController(maxRequestID,socketAddress,gossipConfig);

        setLastUpdateTime();
        System.out.println(Thread.currentThread().getName() + "：新生成了一个节点" + this.IPAddress.toString());

    }


    public InetAddress getIPAddress(){
        return IPAddress;
    }

    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(IPAddress, receivePort);
    }

    //广播来获取集群中节点的相关信息，用于节点状态表的初始化
    public void broadcast(ArrayList<InetSocketAddress> nodeCluster){
        InetSocketAddress iAddress;

        for (int i = 0;i < nodeCluster.size();i++){
            iAddress = nodeCluster.get(i);
            nodeStateTable.putIfAbsent(iAddress,true);
        }
    }

    /*
     * 获取节点的唯一标识，以节点的IP地址作为唯一的标识
     * */
    public String getNodeID(){
        return IPAddress.toString();
    }

    /*
     * 获取主机名
     * */
    public String getHostName(){
        return IPAddress.getHostName();
    }

    /*
     * 获取IP地址
     * */
    /*public InetAddress getIPAddress(){
        return socketAddress.getAddress();
    }*/

    /*
     * 获取端口号
     * */
    /*public int getPort(){
        return socketAddress.getPort();
    }*/

    public int getReceivePort() {
        return receivePort;
    }

    public boolean isFailed() {
        return failed;
    }

    public void setFailed(boolean failed) {
        this.failed = failed;
    }

    public LocalDateTime getLastUpdateTime() {
        return lastUpdateTime;
    }

    public GossipController getGossipController() {
        return gossipController;
    }



    public void setNodeStateTable(ConcurrentHashMap<InetSocketAddress, Boolean> nodeStateTable) {
        this.nodeStateTable = nodeStateTable;
    }

    public GossipConfig getGossipConfig() {
        return gossipConfig;
    }

    public void setGossipConfig(GossipConfig gossipConfig) {
        this.gossipConfig = gossipConfig;
    }

    public ConcurrentHashMap<Long, VectorClock> getVectorClockMap() {
        return vectorClockMap;
    }

    public void setVectorClockMap(ConcurrentHashMap<Long, VectorClock> vectorClockMap) {
        this.vectorClockMap = vectorClockMap;
    }




    public ArrayList<InetSocketAddress> getAliveNodes(){
        int nodeNumber = nodeStateTable.size();
        ArrayList<InetSocketAddress> aliveNodes = new ArrayList<>(nodeNumber);


        for(InetSocketAddress key : nodeStateTable.keySet()){


            if(nodeStateTable.get(key)){
                aliveNodes.add(key);
            }
        }

        return aliveNodes;
    }

    public ArrayList<InetSocketAddress> getFailedNodes(){
        int nodeNumber = nodeStateTable.size();
        ArrayList<InetSocketAddress> failedNodes = new ArrayList<>(nodeNumber);


        for(InetSocketAddress key : nodeStateTable.keySet()){

            if(!nodeStateTable.get(key)){
                failedNodes.add(key);
            }
        }

        return failedNodes;
    }

    public ArrayList<InetSocketAddress> getAllMembers() {

        int initialSize = nodeStateTable.size();
        ArrayList<InetSocketAddress> allMembers = new ArrayList<>(initialSize);

        for (InetSocketAddress key : nodeStateTable.keySet()) {
            allMembers.add(key);
        }

        return allMembers;
    }


    private void initialVectorClockMap(){
        vectorClockMap.putIfAbsent(1L,new VectorClock());
    }

    public void setLastUpdateTime(){
        LocalDateTime updateTime = LocalDateTime.now();
        System.out.println(Thread.currentThread().getName() + "：节点"+ IPAddress.toString() + "在" + updateTime + "时刻" + "发生一次更新。");
        this.lastUpdateTime = updateTime;
    }



    public VectorClock getVectorClock(Long key){
        return vectorClockMap.get(key);
    }

    public void setVectorClock(Long key, VectorClock vectorClock){
        vectorClockMap.put(key,vectorClock);
    }





    /*public void increaseVersion(){
        version++;
        setLastUpdateTime();
    }*/

    /*public void updateVersion(long newVersion){
        if(newVersion > version){
            System.out.println("当前节点" + this.getNodeID() + "的版本号从" + version + "更新为" + newVersion);
            setVersion(newVersion);
            setLastUpdateTime();
        }
    }*/

    /*
     * 检测节点是否故障
     * */
    public void check(){
        //存在异常，lastUpdateTime可能为空
        if(lastUpdateTime == null){
            lastUpdateTime = LocalDateTime.now();
        }

        LocalDateTime failureTime = lastUpdateTime.plus(gossipConfig.failureTimeOut);
        LocalDateTime now = LocalDateTime.now();
        failed = now.isAfter(failureTime);
        if (failed)
            stop();
    }

    public boolean shouldCleanUp(){
        if(failed){
            Duration cleanUpTimeOut = gossipConfig.failureTimeOut.plus(gossipConfig.cleanUpTimeOut);
            LocalDateTime cleanUpTime = lastUpdateTime.plus(cleanUpTimeOut);
            LocalDateTime now=LocalDateTime.now();
            return now.isAfter(cleanUpTime);
        }
        else{
            return false;
        }
    }

    /*public String getNetworkMessage() {
        return "[" + socketAddress.getHostName() +
                ":" + socketAddress.getPort() +
                 "]";
    }*/

    public void updateVectorClock(Long key){
        setLastUpdateTime();
        VectorClock clocks = vectorClockMap.get(key);



        if (clocks == null ){
            clocks = new VectorClock();
        }

        clocks.increaseVersion(/*getNodeID()*/getIPAddress().toString(),System.currentTimeMillis());
        vectorClockMap.putIfAbsent(key,clocks);

        syncQueue.add(key);//压入队列中

    }


    public GossipRequest generateGossipRequest(Long key){
        if (requestID < maxRequestID){
            requestID++;
        }
        else {
            requestID = 1;
        }
        return new GossipRequest(requestID,key,getVectorClock(key),getIPAddress());
    }

    //处理一个请求
    private void processQueue(){

        SendInfo info = gossipController.getSendInfo();

        if(!info.structureIsFull()) {
            //获取一个请求，并移交给下层
            long l1 = System.currentTimeMillis();
            GossipRequest request = generateGossipRequest(syncQueue.peek());
            long l2 = System.currentTimeMillis();
            TimeTest.setGenerateRequestTime(TimeTest.calculate(l1, l2));
            //gossipController.setCurrentRequest(request);

            //insert into test values("a",1,10.0);
            Action action = new Action(
                    OperationType.insert,
                    "test",
                    "test",
                    1,
                    1,
                    3,
                    new String[]{"name","age","num"},
                    new String[]{"String","Integer","Double"},
                    new String[]{"a","1","10.0"});
            request.setAction(action);
            info.getRequestToSend().add(request);

            //获取能够发送的目标节点的IP套接字（IP地址与其对应的接收端口），并移交给下层
            long l3 = System.currentTimeMillis();
            ArrayList<InetSocketAddress> aliveNodes = getAliveNodes();
            long l4 = System.currentTimeMillis();
            TimeTest.setGetAliveNodesTime(TimeTest.calculate(l3, l4));
            //gossipController.setCurrentNodes(aliveNodes.toArray());
            info.getTargets().add(aliveNodes.toArray());
            info.getRequestSendOver().put(request.getRequestID(), false);

            //等待下层把这个请求发送完成（即向所有待发节点都发送成功）


        }
        else{

            try {
                wait(500);
            }catch (InterruptedException e){
                e.printStackTrace();
            }

        }


        //仍然待改进，只有每个节点的请求发送都成功后才能判断这个request是成功的
        /*gossipController.setCurrentRequest(null);
        gossipController.setCurrentNodes(null);
        gossipController.setRequestIsSent(false);*/
        syncQueue.poll();//简单处理，直接从队列中移出
        //需要根据状态表来加速队列的处理，代码在此处添加
        //1. 请求在什么条件下从syncQueue中移出
        //2. 如何判断这个条件 --- 读写仲裁判定通过
        //3. 需要哪些数据结构


    }


    //解析接收到的请求
    private void analyseRequest(GossipRequest gossipRequest){
        long primaryKey = gossipRequest.getKey();
        InetAddress IPAddress1 = gossipRequest.getSourceIPAddress();

        //nodeStateTable.putIfAbsent(socketAddress1,true);

        //如果请求更新的数据不在本地数据库中->新的数据
        if(!vectorClockMap.containsKey(primaryKey)){

        }
        else {
            System.out.println(Thread.currentThread().getName() + "：节点" + IPAddress.toString() + "成功接收到来自" + IPAddress1.toString() + "的向量时钟");
            VectorClock oldClock = getVectorClock(primaryKey);
            long l1=System.currentTimeMillis();
            VectorClock newClock = oldClock.merge(gossipRequest.getVectorClock());//合并向量时钟
            long l2=System.currentTimeMillis();
            TimeTest.setMergeVectorClockTime(TimeTest.calculate(l1,l2));
            //等待后续补充其他对向量时钟更加复杂操作的实现

            vectorClockMap.put(primaryKey,newClock);

        }
        setLastUpdateTime();
    }


    private void startAnalyseThread(){
        new Thread(()->{
            while (!failed){
                while(!gossipController.getReceivedRequestQueue().isEmpty()){
                    GossipRequest front = gossipController.getReceivedRequestQueue().poll();//取队首并删除队首
                    analyseRequest(front);
                }

                try{
                    Thread.sleep(gossipConfig.updateFrequency.toMillis());
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        },"analyseThread").start();
    }

    private void startProcessThread(){
        new Thread(()->{
            while(!failed){
                while(!syncQueue.isEmpty()){
                    long start =System.currentTimeMillis();
                    processQueue();
                    long end = System.currentTimeMillis();
                    TimeTest.setProcessQueueTimeOnce(TimeTest.calculate(start,end));
                }

                try{
                    Thread.sleep(gossipConfig.updateFrequency.toMillis());
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        },"processThread").start();
    }


    public void start(){
        startProcessThread();
        startAnalyseThread();
        gossipController.start();
    }


    public void stop(){
        gossipController.stop();
    }


    /*public void updateListener(){
        Long key = 1L;
        //1. 数据完成更新

        //2. 计算差值并压入队列

        //3. 更新向量时钟
        updateVectorClock(key);
        //4. 启动Gossip进行传输
        gossipController.start(true);
    }

    public void startUpdateListenerThread(){
        new Thread(() -> {
            while(!failed){
                updateListener();
            }
        }).start();
    }*/




}


