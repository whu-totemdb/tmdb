package drz.tmdb.sync.node;

import drz.tmdb.sync.arbitration.ArbitrationController;
import drz.tmdb.sync.config.GossipConfig;
import drz.tmdb.sync.network.GossipController;
import drz.tmdb.sync.network.GossipRequest;
import drz.tmdb.sync.network.Response;
import drz.tmdb.sync.network.SocketService;
import drz.tmdb.sync.node.database.Action;
import drz.tmdb.sync.node.database.DataManager;
import drz.tmdb.sync.node.database.OperationType;
import drz.tmdb.sync.share.ReceiveDataArea;
import drz.tmdb.sync.share.RequestType;
import drz.tmdb.sync.share.SendInfo;
import drz.tmdb.sync.share.SendWindow;
import drz.tmdb.sync.share.WindowEntry;
import drz.tmdb.sync.timeTest.ReceiveTimeTest;
import drz.tmdb.sync.timeTest.SendTimeTest;
import drz.tmdb.sync.util.NetworkUtil;
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
import java.util.concurrent.ConcurrentHashMap;

public class Node implements Serializable {
    //private final InetSocketAddress socketAddress;//节点的IP套接字（IP地址+端口号）
    private final String nodeID;

    private final InetAddress IPAddress;//IP地址

    public static final int broadcastPort = 10010;//广播端口

    private static int requestID = 0;

    private static int maxRequestID = 65536;

    private static int receiveAreaSize = 65536;

    private final int receivePort;//节点的接收端口

    private LocalDateTime lastUpdateTime = null;//节点最新一次更新的时间

    private volatile boolean failed =false;//节点是否故障

    private GossipConfig gossipConfig;

    private ConcurrentHashMap<Long, VectorClock> vectorClockMap;

    private DataManager dataManager;

    private SendWindow sendWindow;//发送窗口，存储待发送请求的数据区

    private ConcurrentHashMap<InetSocketAddress,Boolean> nodeStateTable = new ConcurrentHashMap<>();//节点状态表

    private GossipController gossipController;

    private ArbitrationController arbitrationController;


    //统计使用
    public static HashMap<Integer,SendTimeTest> sendTimeTest = new HashMap<>();//批次号batch_id与测试对象的映射

    public static HashMap<Integer,ArrayList<Integer>> batch_id_map = new HashMap<>();//批次号batch_id与该批请求的映射

    public static HashMap<Integer,ReceiveTimeTest> receiveTimeTest = new HashMap<>();//请求id与接收时间测试对象的映射

    public static int batch_num = 0;



    public Node( String nodeID, InetAddress IPAddress, int receivePort, GossipConfig gossipConfig) {
        //this.socketAddress = socketAddress;
        //this.version = version;
        this.nodeID = nodeID;

        this.IPAddress = IPAddress;
        this.receivePort = receivePort;
        this.gossipConfig = gossipConfig;



        vectorClockMap = new ConcurrentHashMap<>();
        dataManager = new DataManager();
        arbitrationController = new ArbitrationController();


        initialVectorClockMap();//初始化应用数据库

        InetSocketAddress socketAddress = new InetSocketAddress(IPAddress,receivePort);

        if(failed)
            nodeStateTable.putIfAbsent(socketAddress,false);
        else
            nodeStateTable.putIfAbsent(socketAddress,true);


        gossipController = new GossipController(maxRequestID,receiveAreaSize,socketAddress,gossipConfig);

        sendWindow = new SendWindow(65536,10);

        //setLastUpdateTime();
        System.out.println(Thread.currentThread().getName() + "：新生成了一个节点" + this.IPAddress.toString());
        showNodeStateTable();


    }




    public InetAddress getIPAddress(){
        return IPAddress;
    }

    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(IPAddress, receivePort);
    }

    //广播来获取集群中节点的相关信息，用于节点状态表的初始化
    public void broadcast1(ArrayList<InetSocketAddress> nodeCluster){
        InetSocketAddress iAddress;

        for (int i = 0;i < nodeCluster.size();i++){
            iAddress = nodeCluster.get(i);
            nodeStateTable.putIfAbsent(iAddress,true);
        }
    }


    public void broadcast(){
        ArrayList<InetAddress> addresses;

        try {
            addresses = NetworkUtil.getBroadcastAddresses();
            GossipRequest gossipRequest = new GossipRequest(getSocketAddress(),true);
            byte[] data = SocketService.getDataToTransport(gossipRequest);

            for (InetAddress address : addresses){
                try {
                    System.out.println("广播地址为："+address);
                    SocketService.broadcast(address,data);
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

    //更新本地的向量时钟，只有写请求才会调用此函数
    public void updateVectorClock(long key){
        setLastUpdateTime();

        VectorClock clocks = vectorClockMap.get(key);

        if (clocks == null ){
            clocks = new VectorClock();
        }

        clocks.increaseVersion(getNodeID()/*getIPAddress().toString()*/,System.currentTimeMillis());
        vectorClockMap.putIfAbsent(key,clocks);


    }

    public void startDataManageThread(){
        Thread thread = new Thread(() -> {
            while (!failed){
                Action action = dataManager.getAction();

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

                        sendWindow.put(RequestType.readRequest, action);
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

                        updateVectorClock(action.getKey());
                        sendWindow.put(RequestType.writeRequest, action);
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
            if(sendWindow.isEmpty()){
                try {
                    sendWindow.wait();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

            entry = sendWindow.getNextEntry();//取下一个待发送的请求，但不移出数据区
        }

        long key = entry.getAction().getKey();

        if (requestID < maxRequestID){
            requestID++;
        }
        else {
            requestID = 1;
        }

        System.out.println(Thread.currentThread().getName() + "：本次处理的请求的id为" + requestID );

        //获取对应的action对象
        Action action = entry.getAction();

        //insert into test values("a",1,10.0);
        /*Action action = new Action(
                OperationType.insert,
                "test",
                "test",
                1,
                1,
                3,
                new String[]{"name", "age", "num"},
                new String[]{"String", "Integer", "Double"},
                new String[]{"a", "1", "10.0"});*/


        //生成一个仲裁判定器




        GossipRequest request = new GossipRequest(requestID,key,action,getVectorClock(key),getSocketAddress(),false);
        request.batch_id = batch_num;

        return request;
    }

    //处理一个请求
    private void process(GossipRequest request){

        ArrayList<InetSocketAddress> aliveNodes = getAliveNodes();

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
            info.notifyAll();//如果发送线程因为临界区为空而等待，唤醒发送线程
        }



        //仍然待改进，只有每个节点的请求发送都成功后才能判断这个request是成功的
        synchronized (sendWindow){
            sendWindow.remove();//窗口移出一个请求
            sendWindow.notifyAll();
        }
        //需要根据状态表来加速队列的处理，代码在此处添加
        //1. 请求在什么条件下从syncQueue中移出
        //2. 如何判断这个条件 --- 读写仲裁判定通过
        //3. 需要哪些数据结构


    }


    //解析一次请求
    private void analyseRequest(){
        GossipRequest gossipRequest;
        ReceiveDataArea receiveDataArea = gossipController.getReceiveDataArea();

        synchronized (receiveDataArea.getReceivedRequestQueue()) {
            if(receiveDataArea.requestQueueEmpty()){
                try {
                    receiveDataArea.getReceivedRequestQueue().wait();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }

            }

            gossipRequest = receiveDataArea.getReceivedRequestQueue().poll();//取队首并删除队首
            receiveDataArea.getReceivedRequestQueue().notifyAll();
        }

        //接收到其他节点发来的广播请求，向该节点返回响应，响应中包含本节点的IP套接字信息（IP地址+接收端口号）
        if(gossipRequest.isBroadcast()) {
            InetSocketAddress source = getSocketAddress();//本机的IP套接字
            //System.out.println("本机的IP套接字为："+source);
            InetSocketAddress target = gossipRequest.getSourceIPAddress();
            nodeStateTable.putIfAbsent(target,true);
            showNodeStateTable();

            Thread sendResponseThread = new Thread(() -> {
                Response response = new Response(source,target,true);
                byte[] data = SocketService.getDataToTransport(response);

                try {
                    gossipController.socketService.sendToTargetNode(target,data);
                    System.out.println(Thread.currentThread().getName() + "成功发送响应");
                }catch (IOException e){
                    e.printStackTrace();
                }

            },"sendResponseThread");
            sendResponseThread.start();
        }
        else {
            //不是广播请求
            long primaryKey = gossipRequest.getKey();//主键
            InetSocketAddress socketAddress1 = gossipRequest.getSourceIPAddress();
            int id = gossipRequest.getRequestID();//请求的id号

            nodeStateTable.put(socketAddress1,true);

            //如果请求更新的数据不在本地数据库中->新的数据
            if (!vectorClockMap.containsKey(primaryKey)) {

            } else {
                System.out.println(Thread.currentThread().getName() + "：节点" + IPAddress.toString() + "成功接收到来自" + socketAddress1.getAddress().toString() + "的向量时钟");
                VectorClock oldClock = getVectorClock(primaryKey);
                long l1 = System.currentTimeMillis();
                VectorClock newClock = oldClock.merge(gossipRequest.getVectorClock());//合并向量时钟
                long l2 = System.currentTimeMillis();
                receiveTimeTest.get(id).setMergeVectorClockTime(SendTimeTest.calculate(l1, l2));
                //等待后续补充其他对向量时钟更加复杂操作的实现

                vectorClockMap.put(primaryKey, newClock);

            }
            setLastUpdateTime();
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

        if (response.isBroadcast()){
            //如果是广播请求的响应
            InetSocketAddress inetSocketAddress = response.getSource();//获取其他节点的IP套接字（IP地址+接收端口号）
            nodeStateTable.putIfAbsent(inetSocketAddress,true);//更新节点状态表
            showNodeStateTable();
        }
        else {

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
                GossipRequest request = generateGossipRequest();

                if (!sendTimeTest.containsKey(request.batch_id)) {
                    SendTimeTest s = new SendTimeTest();
                    sendTimeTest.put(request.batch_id, s);
                    batch_id_map.put(request.batch_id,new ArrayList<>());
                }

                long start =System.currentTimeMillis();
                process(request);
                long end = System.currentTimeMillis();
                sendTimeTest.get(request.batch_id).setProcessQueueTimeOnce(SendTimeTest.calculate(start,end));

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
        startAnalyseRequestThread();
        startAnalyseResponseThread();
        startDataManageThread();
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

    public void showNodeStateTable(){
        System.out.println("本节点保存的集群内各IP地址的信息为：");
        for (InetSocketAddress i : nodeStateTable.keySet()){
            System.out.println(i);
        }
    }


}


