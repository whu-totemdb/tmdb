package drz.tmdb.sync.node;

import android.app.AlertDialog;
import android.content.DialogInterface;
import android.os.Looper;

import drz.tmdb.sync.Sync;
import drz.tmdb.sync.arbitration.Arbitration;
import drz.tmdb.sync.arbitration.ArbitrationController;
import drz.tmdb.sync.config.GossipConfig;
import drz.tmdb.sync.network.Deviation;
import drz.tmdb.sync.network.GossipController;
import drz.tmdb.sync.network.GossipRequest;
import drz.tmdb.sync.network.Response;
import drz.tmdb.sync.network.SocketService;
import drz.tmdb.sync.node.database.Action;
import drz.tmdb.sync.node.database.DataManager;
import drz.tmdb.sync.node.database.OperationType;
import drz.tmdb.sync.share.ReceiveDataArea;
import drz.tmdb.sync.share.RequestType;
import drz.tmdb.sync.share.ResponseType;
import drz.tmdb.sync.share.SendInfo;
import drz.tmdb.sync.share.SendWindow;
import drz.tmdb.sync.share.WindowEntry;
import drz.tmdb.sync.timeTest.Cost;
import drz.tmdb.sync.timeTest.ReceiveTimeTest;
import drz.tmdb.sync.timeTest.SendTimeTest;
import drz.tmdb.sync.timeTest.TimeStore;
import drz.tmdb.sync.timeTest.TimeTest;
import drz.tmdb.sync.timer.MyTimer;
import drz.tmdb.sync.util.NetworkUtil;
import drz.tmdb.sync.vectorClock.ClockEntry;
import drz.tmdb.sync.vectorClock.VectorClock;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

public class Node implements Serializable {
    //private final InetSocketAddress socketAddress;//节点的IP套接字（IP地址+端口号）
    private static boolean test = true;

    private final String nodeID;

    private final InetAddress IPAddress;//IP地址

    public static final int broadcastPort = 12000;//广播端口

    private int requestNum = 0;

    private int maxRequestNum = 65536;

    private int receiveAreaSize = 65536;

    private final int receivePort;//节点的接收端口

    private LocalDateTime lastUpdateTime = null;//节点最新一次更新的时间

    private volatile boolean failed =false;//节点是否故障

    private GossipConfig gossipConfig;

    private ConcurrentHashMap<Long, VectorClock> vectorClockMap;


    //private ConcurrentHashMap<Long, ClockEntry> clockEntries;

    private DataManager dataManager;

    private SendWindow sendWindow;//发送窗口，存储待发送请求的数据区

    /*private ConcurrentHashMap<InetSocketAddress,Boolean> nodeStateTable = new ConcurrentHashMap<>();//节点状态表

    private ConcurrentHashMap<String,InetSocketAddress> nodes = new ConcurrentHashMap<>();//节点ID与其IP地址的映射表

    private ConcurrentHashMap<String,Long> errors = new ConcurrentHashMap<>();//记录下本节点与每个其他节点的时钟偏差*/

    private ConcurrentHashMap<String,NodeInfo> cluster = new ConcurrentHashMap<>();

    private GossipController gossipController;

    private ArbitrationController arbitrationController;

    private long startTime;

    private long endTime;


    //统计使用
    /*public static HashMap<Integer,SendTimeTest> sendTimeTest = new HashMap<>();//批次号batch_id与测试对象的映射

    public static HashMap<Integer,ArrayList<String>> batch_id_map = new HashMap<>();//批次号batch_id与该批请求的映射

    public static HashMap<String,ReceiveTimeTest> receiveTimeTest = new HashMap<>();//请求id与接收时间测试对象的映射

    public static int batch_num = 0;*/

    public static TreeMap<String, TimeStore> timeMap = new TreeMap<>();



    public Node( String nodeID, InetAddress IPAddress, int receivePort, GossipConfig gossipConfig) {
        //this.socketAddress = socketAddress;
        //this.version = version;
        this.nodeID = nodeID;

        this.IPAddress = IPAddress;
        this.receivePort = receivePort;
        this.gossipConfig = gossipConfig;

        vectorClockMap = new ConcurrentHashMap<>();
        //clockEntries = new ConcurrentHashMap<>();
        dataManager = new DataManager();
        arbitrationController = new ArbitrationController();

        //初始化仲裁模块的配置参数
        Arbitration.initialConfig(gossipConfig.copyNum,gossipConfig.minWriteNum,gossipConfig.minReadNum);


        initialVectorClockMap();//初始化应用数据库的向量时钟表

        InetSocketAddress socketAddress = getSocketAddress();

        NodeInfo info;
        if(failed){
            info = new NodeInfo(socketAddress,0,NodeState.fail);
            //nodeStateTable.putIfAbsent(socketAddress,false);
        }
        else {
            info = new NodeInfo(socketAddress,0,NodeState.active);
            //nodeStateTable.putIfAbsent(socketAddress, true);
        }
        cluster.put(nodeID,info);
        //nodes.put(nodeID,socketAddress);

        sendWindow = new SendWindow(gossipConfig.maxSendNum,gossipConfig.windowSize);

        //setLastUpdateTime();
        System.out.println(Thread.currentThread().getName() + "：新生成了一个节点" + this.IPAddress.toString());
        showCluster();
        gossipController = new GossipController(this,maxRequestNum,receiveAreaSize,socketAddress,gossipConfig);
        //this.start();
    }


    public Node(InetAddress IPAddress,
                int receivePort,
                GossipConfig gossipConfig,
                int requestNum,
                String nodeID,
                LocalDateTime lastUpdateTime,
                ConcurrentHashMap<Long, VectorClock> vectorClockMap,
                DataManager dataManager,
                SendWindow sendWindow,
                ConcurrentHashMap<String, NodeInfo> cluster,
                ArbitrationController arbitrationController,
                SendInfo sendInfo,
                ReceiveDataArea receiveDataArea)
    {
        this.IPAddress = IPAddress;
        this.receivePort = receivePort;
        this.gossipConfig = gossipConfig;

        this.requestNum = requestNum;
        this.nodeID = nodeID;
        this.lastUpdateTime = lastUpdateTime;
        this.vectorClockMap = vectorClockMap;
        this.dataManager = dataManager;
        this.sendWindow = sendWindow;
        this.cluster = cluster;
        this.arbitrationController = arbitrationController;

        //初始化仲裁模块的配置参数
        Arbitration.initialConfig(gossipConfig.copyNum,gossipConfig.minWriteNum,gossipConfig.minReadNum);

        this.gossipController = new GossipController(this,getSocketAddress(),gossipConfig,sendInfo,receiveDataArea);
    }





    public static boolean isTest() {
        return test;
    }

    public static void setTest(boolean test) {
        Node.test = test;
    }



    public InetAddress getIPAddress(){
        return IPAddress;
    }

    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(IPAddress, receivePort);
    }

    //广播来获取集群中节点的相关信息，用于节点状态表的初始化
    /*public void broadcast1(ArrayList<InetSocketAddress> nodeCluster){
        InetSocketAddress iAddress;

        for (int i = 0;i < nodeCluster.size();i++){
            iAddress = nodeCluster.get(i);
            nodeStateTable.putIfAbsent(iAddress,true);
        }
    }*/


    public void broadcast(){
        ArrayList<InetAddress> addresses;

        try {
            addresses = NetworkUtil.getBroadcastAddresses();
            GossipRequest gossipRequest = new GossipRequest(RequestType.broadcastRequest, getNodeID(), getSocketAddress());
            //byte[] data = SocketService.getDataToTransport(gossipRequest);

            for (InetAddress address : addresses){
                try {
                    System.out.println("广播地址为："+address);
                    SocketService.broadcast(address,gossipRequest);
                }catch (IOException e){
                    e.printStackTrace();
                }

            }
        }catch (SocketException e){
            e.printStackTrace();
        }


    }

    /*
     * 获取节点的唯一标识
     * */
    public String getNodeID(){
        return nodeID;
    }

    /*
     * 获取主机名
     * */
    public String getHostName(){
        return IPAddress.getHostName();
    }



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

    public DataManager getDataManager() {
        return dataManager;
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

    public int getRequestNum() {
        return requestNum;
    }

    public SendWindow getSendWindow() {
        return sendWindow;
    }

    public ConcurrentHashMap<String, NodeInfo> getCluster() {
        return cluster;
    }

    public ArbitrationController getArbitrationController() {
        return arbitrationController;
    }




    public ArrayList<InetSocketAddress> getAliveNodes(){
        int nodeNumber = cluster.size();
        ArrayList<InetSocketAddress> aliveNodes = new ArrayList<>(nodeNumber);

        NodeInfo info;
        for(String key : cluster.keySet()){

            info = cluster.get(key);
            if(info.state == NodeState.active){
                aliveNodes.add(info.socketAddress);
            }
        }

        return aliveNodes;
    }

    public ArrayList<InetSocketAddress> getFailedNodes(){
        int nodeNumber = cluster.size();
        ArrayList<InetSocketAddress> failedNodes = new ArrayList<>(nodeNumber);

        NodeInfo info;
        for(String key : cluster.keySet()){
            info = cluster.get(key);
            if(info.state == NodeState.fail){
                failedNodes.add(info.socketAddress);
            }
        }

        return failedNodes;
    }

    public ArrayList<InetSocketAddress> getAllMembers() {

        int initialSize = cluster.size();
        ArrayList<InetSocketAddress> allMembers = new ArrayList<>(initialSize);


        for (String key : cluster.keySet()) {
            allMembers.add(cluster.get(key).socketAddress);
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

    //更新本地的向量时钟，只有写请求才会调用此函数
    public void updateVectorClock(long key){
        setLastUpdateTime();

        VectorClock clocks = vectorClockMap.get(key);

        if (clocks == null ){
            clocks = new VectorClock();
        }

        clocks.increaseVersion(getNodeID(),System.currentTimeMillis());
        //vectorClockMap.putIfAbsent(key,clocks);
        vectorClockMap.put(key,clocks);

    }

    public void startDataManageThread(){
        Thread thread = new Thread(() -> {
            while (!failed){
                Action action = dataManager.getAction();
                System.out.println(Thread.currentThread().getName()+"：成功将一个同步操作压入发送窗口");


                startTime = System.currentTimeMillis();
                long start = startTime;

                long key = action.getKey();

                if (requestNum < maxRequestNum){
                    requestNum++;
                }
                else {
                    requestNum = 1;
                }

                String requestID = getNodeID() + requestNum;

                System.out.println(Thread.currentThread().getName() + "：本次处理的请求的id为" + requestID );


                if(action.getOp() == OperationType.select){
                    //读请求
                    synchronized (sendWindow) {
                        if (sendWindow.isFull()){
                            try {
                                sendWindow.wait();
                            }catch (InterruptedException e){
                                e.printStackTrace();
                            }
                        }

                        //生成一个仲裁判定器
                        Arbitration arbitration =new Arbitration(RequestType.readRequest);
                        arbitrationController.putArbitration(requestID,arbitration);
                        System.out.println(getNodeID()+"生成一个仲裁器");


                        GossipRequest request = new GossipRequest(RequestType.readRequest,getNodeID(),requestID,key,action,getVectorClock(key),getSocketAddress());
                        //request.batch_id = batch_num;
                        long end =System.currentTimeMillis();

                        if (Node.isTest()) {
                            System.out.println("生成一次请求耗费："+(end - start)+"ms");
                            TimeTest.putInCostTreeMap(requestID,"生成请求耗时",(end-start));

                            timeMap.put(requestID,new TimeStore());
                            timeMap.get(requestID).requestStartTime = start;
                        }


                        //sendWindow.put(RequestType.readRequest, action);
                        sendWindow.put(RequestType.readRequest,request);
                        sendWindow.notifyAll();
                    }
                }
                else{//写请求

                    synchronized (sendWindow) {
                        if (sendWindow.isFull()){
                            try {
                                sendWindow.wait();
                            }catch (InterruptedException e){
                                e.printStackTrace();
                            }
                        }

                        //生成一个仲裁判定器
                        Arbitration arbitration =new Arbitration(RequestType.writeRequest);
                        arbitrationController.putArbitration(requestID,arbitration);
                        System.out.println(getNodeID()+"生成一个仲裁器");


                        GossipRequest request = new GossipRequest(RequestType.writeRequest,getNodeID(),requestID,key,action,getVectorClock(key),getSocketAddress());
                        //request.batch_id = batch_num;
                        long end =System.currentTimeMillis();

                        if (Node.isTest()) {
                            System.out.println("生成一次请求耗费："+(end - start)+"ms");
                            TimeTest.putInCostTreeMap(requestID,"生成请求耗时",(end-start));

                            timeMap.put(requestID,new TimeStore());
                            timeMap.get(requestID).requestStartTime = start;
                        }


                        updateVectorClock(action.getKey());
                        //sendWindow.put(RequestType.writeRequest, action);
                        sendWindow.put(RequestType.writeRequest, request);
                        sendWindow.notifyAll();
                    }

                }
            }
        },"dataManageThread");
        thread.start();
    }


    public GossipRequest generateGossipRequest(){
        //获取一个数据区内待处理请求
        WindowEntry entry;
        synchronized (sendWindow) {
            if(sendWindow.noNewElement()){
                try {
                    sendWindow.wait();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

            entry = sendWindow.getNextEntry();//取下一个待发送的请求，但不移出数据区
        }

        startTime = System.currentTimeMillis();
        long start = startTime;

        long key = entry.getAction().getKey();

        if (requestNum < maxRequestNum){
            requestNum++;
        }
        else {
            requestNum = 1;
        }

        String requestID = getNodeID() + requestNum;

        System.out.println(Thread.currentThread().getName() + "：本次处理的请求的id为" + requestID );

        //获取对应的action对象
        Action action = entry.getAction();

        //生成一个仲裁判定器
        Arbitration arbitration =new Arbitration(entry.getRequestType());
        arbitrationController.putArbitration(requestID,arbitration);
        System.out.println(getNodeID()+"生成一个仲裁器");


        GossipRequest request = new GossipRequest(entry.getRequestType(),getNodeID(),requestID,key,action,getVectorClock(key),getSocketAddress());
        //request.batch_id = batch_num;
        long end =System.currentTimeMillis();

        if (Node.isTest()) {
            System.out.println("生成一次请求耗费："+(end - start)+"ms");
            TimeTest.putInCostTreeMap(requestID,"生成请求耗时",(end-start));

            timeMap.put(requestID,new TimeStore());
            timeMap.get(requestID).requestStartTime = start;
        }
        return request;
    }

    //处理一个请求
    private void process(GossipRequest request,int index){

        ArrayList<InetSocketAddress> aliveNodes = getAliveNodes();

        //ArrayList<InetSocketAddress> aliveNodes = getAllMembers();


        SendInfo info = gossipController.getSendInfo();
        synchronized (info) {
            if (info.structureIsFull()) {
                try {
                    info.wait();//如果临界区已满，处理线程等待发送线程处理完一个请求后唤醒
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            info.getRequestToSend().add(request);
            info.getTargets().add(aliveNodes.toArray());
            info.getIndexMap().put(request.getRequestID(),index);
            info.notifyAll();//如果发送线程因为临界区为空而等待，唤醒发送线程
        }


        /*synchronized (sendWindow){
            sendWindow.remove();//窗口移出一个请求
            sendWindow.notifyAll();
        }*/



    }


    //解析一次请求
    private void analyseRequest(){
        GossipRequest gossipRequest;
        ReceiveDataArea receiveDataArea = gossipController.getReceiveDataArea();

        long l1;
        synchronized (receiveDataArea.getReceivedRequestQueue()) {
            if(receiveDataArea.requestQueueEmpty()){
                try {
                    receiveDataArea.getReceivedRequestQueue().wait();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }

            }
            l1 = System.currentTimeMillis();
            gossipRequest = receiveDataArea.getReceivedRequestQueue().poll();//取队首并删除队首
            receiveDataArea.getReceivedRequestQueue().notifyAll();
        }

        //接收到其他节点发来的广播请求，向该节点返回响应，响应中包含本节点的IP套接字信息（IP地址+接收端口号）
        if(gossipRequest.getRequestType() == RequestType.broadcastRequest) {
            InetSocketAddress source = getSocketAddress();//本机的IP套接字
            //System.out.println("本机的IP套接字为："+source);
            String sourceNodeID = gossipRequest.getNodeID();
            InetSocketAddress target = gossipRequest.getSourceIPAddress();

            if (cluster.get(sourceNodeID)==null){
                //如果不存在该nodeID
                NodeInfo info = new NodeInfo(target,NodeState.active);
                info.startTimer(cluster,sourceNodeID);
                cluster.put(sourceNodeID,info);
            }
            else {
                NodeInfo info = cluster.get(sourceNodeID);
                info.state = NodeState.active;
                info.socketAddress = target;
                info.lastUpdateTime = System.currentTimeMillis();
            }
            /*nodeStateTable.putIfAbsent(target,true);
            nodes.put(sourceNodeID,target);*/
            showCluster();

            Thread sendBroadcastResponseThread = new Thread(() -> {
                Response response = new Response(getNodeID(),gossipRequest.getRequestID(),ResponseType.broadcastResponse,source,target);
                response.setReceiveTime(gossipRequest.receiveTime);//设置广播请求的接收时刻
                try {
                    gossipController.socketService.sendToTargetNode(target,response);
                    System.out.println(Thread.currentThread().getName() + "成功发送响应");
                }catch (IOException e){
                    e.printStackTrace();
                }

            },"sendBroadcastResponseThread");
            sendBroadcastResponseThread.start();
        }
        else {
            //不是广播请求
            long primaryKey = gossipRequest.getKey();//主键
            InetSocketAddress sourceAddress = gossipRequest.getSourceIPAddress();
            String id = gossipRequest.getRequestID();//请求的id号
            String sourceNodeID = gossipRequest.getNodeID();

            if (cluster.get(sourceNodeID)==null){
                //如果不存在该nodeID
                NodeInfo info = new NodeInfo(sourceAddress,NodeState.active);
                cluster.put(sourceNodeID,info);
            }
            else {
                NodeInfo info = cluster.get(sourceNodeID);
                info.state = NodeState.active;
                info.socketAddress = sourceAddress;
                info.lastUpdateTime = System.currentTimeMillis();
            }
            long l2 =System.currentTimeMillis();
            if (Node.isTest()){
                //TimeTest.putInCostTreeMap(id,"请求传播时延",gossipRequest.receiveTime+cluster.get(sourceNodeID).error-gossipRequest.sendTime);
                TimeTest.putInCostTreeMap(id,"解析请求信息耗时",(l2-l1));
            }
            /*nodeStateTable.put(sourceAddress,true);
            nodes.put(sourceNodeID,sourceAddress);*/

            if(gossipRequest.getRequestType() == RequestType.readRequest){
                //读请求
                new Thread(() -> {
                    //下面代码段为根据主键primaryKey获取本节点对应数据的Action对象（待补充）
                    /*Action action;

                    //封装为一个Response对象
                    Response response = new Response(
                            id,
                            ResponseType.readResponse,
                            action,
                            vectorClockMap.get(primaryKey),
                            getSocketAddress(),
                            sourceAddress);

                    try {
                        gossipController.socketService.sendToTargetNode(sourceAddress,SocketService.getDataToTransport(response));
                        System.out.println(Thread.currentThread().getName() + "成功发送读成功响应");
                    }catch (IOException e){
                        e.printStackTrace();
                    }*/

                }).start();
            }else {
                //写请求
                new Thread(() -> {
                    VectorClock otherVectorClock = gossipRequest.getVectorClock();
                    VectorClock localVectorClock = vectorClockMap.get(primaryKey);//本地对应的向量时钟
                    long l3 = System.currentTimeMillis();
                    switch (VectorClock.compare(localVectorClock,otherVectorClock)){
                        case Equal:
                            System.out.println(Thread.currentThread().getName()+"：两个向量时钟相等");
                            break;
                        case Before:
                            System.out.println(Thread.currentThread().getName()+"：先后顺序为before");
                            //接收到的请求对应的版本是更新的
                            Action action1 = gossipRequest.getAction();
                            //将action1应用到本地数据库中（待补充）

                            //本地的向量时钟更新
                            System.out.println("原来的向量时钟："+localVectorClock.showVectorClock());
                            System.out.println("更新后的向量时钟："+otherVectorClock.showVectorClock());
                            vectorClockMap.put(primaryKey,otherVectorClock);
                            break;
                        case After:
                            System.out.println(Thread.currentThread().getName()+"：先后顺序为after");
                            System.out.println("接收的数据因晚于本地版本被丢弃，向量时钟不变化");
                            break;
                        case Parallel:
                            System.out.println(Thread.currentThread().getName()+"：先后顺序为parallel");
                            long loss;
                            NodeInfo info = cluster.get(sourceNodeID);

                            if(info==null){
                                loss = 0;
                            }else {
                                loss = info.error;
                            }

                            if(localVectorClock.getTimestamp() +  loss < otherVectorClock.getTimestamp()){
                                //本地的版本为旧版本
                                Action action2 = gossipRequest.getAction();
                                //将action2应用到本地数据库中（待补充）

                                //本地的向量时钟更新
                                System.out.println("原来的向量时钟："+localVectorClock.showVectorClock());
                                vectorClockMap.put(primaryKey,otherVectorClock);
                                System.out.println("更新后的向量时钟："+vectorClockMap.get(primaryKey).showVectorClock());
                            }
                            break;
                    }
                    long l4 = System.currentTimeMillis();
                    if (Node.isTest()){
                        TimeTest.putInCostTreeMap(id,"一次向量时钟比较耗时",(l4-l3));
                    }

                    //发送一个写成功的响应
                    Response response = new Response(
                            getNodeID(),
                            id,
                            ResponseType.writeResponse,
                            getSocketAddress(),
                            sourceAddress);
                    if (Node.isTest()) {
                        response.setCost(TimeTest.getCosts(id));
                        response.requestProcessTime = -gossipRequest.receiveTime;
                    }
                    try {
                        gossipController.socketService.sendToTargetNode(sourceAddress,response);
                        System.out.println(Thread.currentThread().getName() + "成功发送写成功响应");
                    }catch (IOException e){
                        e.printStackTrace();
                    }

                }).start();

            }

            setLastUpdateTime();
        }
    }

    //仲裁判定
    private void judge(String id){
        long l1 = System.currentTimeMillis();
        long l2;
        Arbitration arbitration = arbitrationController.getArbitrationByID(id);
        if(arbitration.getRequestType() == RequestType.readRequest){
            if (arbitration.readSuccess()){

                System.out.println(Thread.currentThread().getName()+"：读成功");
                //读成功，调用向量时钟算法处理收集到的所有请求，找到其中最新的版本，应用到本地并向用户返回结果
                Action latestAction = VectorClock.getLatestVersion(arbitrationController.getResponsesByID(id));
                arbitrationController.deleteArbitration(id);
                //下面是向用户返回和应用到本地的相关代码

                //显示
                if (Node.isTest()){
                    endTime = System.currentTimeMillis();
                    TimeTest.putInCostTreeMap(id,"请求"+id+"同步全过程耗时",(endTime-startTime));
                    //TimeTest.showCost(id);
                    Looper.prepare();
                    AlertDialog.Builder builder=new AlertDialog.Builder(Sync.getContext());
                    builder.setMessage(/*TimeTest.showCost(id)*/"")
                            .setPositiveButton("确定", new DialogInterface.OnClickListener() {
                                @Override
                                public void onClick(DialogInterface dialogInterface, int i) {
                                    System.out.println("确认");
                                }
                            })
                            .create()
                            .show();
                    Looper.loop();

                }

            }
            else {
                l2 = System.currentTimeMillis();
                if (Node.isTest()){
                    TimeTest.putInCostTreeMap(id,"一次仲裁判定耗时",(l2-l1));
                }
            }
        }
        else {

            if (arbitration.writeSuccess()){
                System.out.println(Thread.currentThread().getName()+"：写成功");
                //写成功，则此次同步认为完成（但可能还有部分节点还没收到）
                arbitrationController.deleteArbitration(id);
                //返回写成功的代码

                //显示
                if (Node.isTest()){
                    endTime = System.currentTimeMillis();
                    timeMap.get(id).endTime = endTime;
                    TimeTest.putInCostTreeMap(id,"请求"+id+"同步全过程耗时",(endTime-startTime));
                    TimeTest.showCost(id);
                    //timeMap.get(id).show();

                    Looper.prepare();
                    AlertDialog.Builder builder=new AlertDialog.Builder(Sync.getContext());
                    builder.setMessage(timeMap.get(id).show())
                            .setPositiveButton("确定", new DialogInterface.OnClickListener() {
                                @Override
                                public void onClick(DialogInterface dialogInterface, int i) {
                                    System.out.println("确认");
                                }
                            })
                            .create()
                            .show();
                    Looper.loop();
                }
            }
            else {
                l2 = System.currentTimeMillis();
                if (Node.isTest()){
                    TimeTest.putInCostTreeMap(id,"一次仲裁判定耗时",(l2-l1));
                }
            }

        }
    }

    private void analyseResponse(){
        Response response;
        ReceiveDataArea receiveDataArea = gossipController.getReceiveDataArea();

        synchronized (receiveDataArea.getReceivedResponseQueue()){
            if (receiveDataArea.responseQueueEmpty()){
                try {
                    receiveDataArea.getReceivedResponseQueue().wait();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }

            }

            response = receiveDataArea.getReceivedResponseQueue().poll();
            receiveDataArea.getReceivedResponseQueue().notifyAll();
        }

        if (response.getResponseType() == ResponseType.broadcastResponse){
            //如果是广播请求的响应
            InetSocketAddress inetSocketAddress = response.getSource();//获取其他节点的IP套接字（IP地址+接收端口号）
            String otherNodeID = response.getNodeID();

            Deviation.putTimeCollection(otherNodeID,response.getReceiveTime(),response.getSendTime());
            long error = Deviation.getError(otherNodeID);
            System.out.println(getNodeID()+"与"+otherNodeID+"之间的误差为："+error+"ms");
            //errors.put(otherNodeID,error);

            if (cluster.get(otherNodeID)==null){
                //如果不存在该nodeID
                NodeInfo info = new NodeInfo(inetSocketAddress,error,NodeState.active);
                info.startTimer(cluster,otherNodeID);
                cluster.put(otherNodeID,info);
            }
            else {
                NodeInfo info = cluster.get(otherNodeID);
                info.state = NodeState.active;
                info.socketAddress = inetSocketAddress;
                info.error = error;
                info.lastUpdateTime = System.currentTimeMillis();
            }
            /*nodes.put(otherNodeID,inetSocketAddress);
            nodeStateTable.putIfAbsent(inetSocketAddress,true);//更新节点状态表*/
            showCluster();

        }
        else {
            String id = response.getRequestID();
            gossipController.sendDataConcurrentHashMap.get(id).responseReceived.put(response.getSource(),true);

            if (Node.isTest()){
                TimeTest.addCosts(id,response.getCost());
                TimeTest.putInCostTreeMap(id,"响应传播时延",response.getReceiveTime()+cluster.get(response.getNodeID()).error - response.getSendTime());
            }

            arbitrationController.putReceivedResponse(id,response);
            Thread judgeThread = new Thread(() -> {
                judge(id);

            },"judgeThread");
            judgeThread.start();
        }
    }

    private void startAnalyseRequestThread(){
        new Thread(()->{
            while (!failed){

                analyseRequest();

                try{
                    Thread.sleep(gossipConfig.updateFrequency.toMillis());
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        },"analyseRequestThread").start();
    }

    private void startAnalyseResponseThread(){
        new Thread(()->{
            while (!failed){
                analyseResponse();

                try{
                    Thread.sleep(gossipConfig.updateFrequency.toMillis());
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        },"analyseResponseThread").start();
    }

    private void startProcessThread(){
        new Thread(()->{
            while(!failed){

                //获取一个数据区内待处理请求
                WindowEntry entry;
                int index;
                synchronized (sendWindow) {
                    if(sendWindow.noNewElement() || sendWindow.noNewElementInWindow()){
                        try {
                            sendWindow.wait();
                        }catch (InterruptedException e){
                            e.printStackTrace();
                        }
                    }
                    if (sendWindow.noNewElement() || sendWindow.noNewElementInWindow())
                        continue;

                    index = sendWindow.getCurrentIndex();//当前请求处于发送窗口内的索引值
                    entry = sendWindow.getNextEntry();//取下一个待发送的请求，但不移出数据区
                }


                //GossipRequest request = generateGossipRequest();
                GossipRequest request = entry.getGossipRequest();

                /*if (!sendTimeTest.containsKey(request.batch_id)) {
                    SendTimeTest s = new SendTimeTest();
                    sendTimeTest.put(request.batch_id, s);
                    batch_id_map.put(request.batch_id,new ArrayList<>());
                }*/

                process(request,index);

                //sendTimeTest.get(request.batch_id).setProcessQueueTimeOnce(SendTimeTest.calculate(start,end));

                try{
                    Thread.sleep(gossipConfig.updateFrequency.toMillis());
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        },"processThread").start();
    }

    private void startBroadcastThread(){
        new Thread(() -> {
            while (!failed) {
                broadcast();

                try{
                    Thread.sleep(gossipConfig.broadcastFrequency.toMillis());
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        },"broadcastThread").start();
    }

    public void start(){
        startProcessThread();
        startAnalyseRequestThread();
        startAnalyseResponseThread();
        startDataManageThread();
        startBroadcastThread();
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

    public void showCluster(){
        System.out.println("本节点保存的集群内各节点的信息为：");
        for (String i : cluster.keySet()){
            NodeInfo info = cluster.get(i);
            System.out.println(i+"-"+info.socketAddress+"-"+info.state+":"+info.error+"ms");
        }
    }


}


