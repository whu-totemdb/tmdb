package drz.tmdb.sync.network;

import drz.tmdb.sync.config.GossipConfig;
import drz.tmdb.sync.node.Node;
import drz.tmdb.sync.share.ReceiveDataArea;
import drz.tmdb.sync.share.SendInfo;
import drz.tmdb.sync.timer.MyTimer;
import drz.tmdb.sync.timer.SendData;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;


public class GossipController implements Serializable {
    private final Node node;

    private final InetSocketAddress socketAddress;

    public SocketService socketService;

    //private Node currentNode = null;//当前节点

    //private Object[] currentNodes;//当前节点已知集群中的其他活跃节点的套接字地址集合

    //private ConcurrentHashMap<String,Node> nodes = new ConcurrentHashMap<>();

    //private volatile GossipRequest currentRequest = null;

    //private volatile boolean requestIsSent = false;
    private SendInfo sendInfo;

    private ReceiveDataArea receiveDataArea;

    private boolean stop = false;

    private GossipConfig gossipConfig = null;

    public ConcurrentHashMap<String, SendData> sendDataConcurrentHashMap = new ConcurrentHashMap<>();



    public GossipController(Node node, int sendInfoSize, int receiveDataAreaSize, InetSocketAddress socketAddress, GossipConfig gossipConfig) {
        this.node = node;
        this.socketAddress = socketAddress;
        this.gossipConfig = gossipConfig;

        this.socketService = new SocketService(socketAddress.getPort());
        //currentNode = self;

        sendInfo = new SendInfo(sendInfoSize);
        receiveDataArea = new ReceiveDataArea(receiveDataAreaSize);
        //currentNode = new Node(socketAddress,0,gossipConfig);
        //nodes.putIfAbsent(socketAddress.toString(),self);

    }

    public GossipController(
            Node node,
            InetSocketAddress socketAddress,
            GossipConfig gossipConfig,
            SendInfo sendInfo,
            ReceiveDataArea receiveDataArea
            ) {
        this.node = node;
        this.socketAddress = socketAddress;
        this.gossipConfig = gossipConfig;

        this.sendInfo = sendInfo;
        this.receiveDataArea = receiveDataArea;

        this.socketService = new SocketService(socketAddress.getPort());
    }


    public SendInfo getSendInfo() {
        return sendInfo;
    }

    public ReceiveDataArea getReceiveDataArea() {
        return receiveDataArea;
    }



    public void start(){

        startSendThread();
        startReceiveThread();
        //startFailureDetectionThread();
        //printNodes();

    }

    /*
     * 获取集群中所有活跃的节点的套接字地址
     * */
    /*public ArrayList<InetSocketAddress> getAliveNodes(){
        int nodeNumber = nodes.size();
        ArrayList<InetSocketAddress> aliveNodes = new ArrayList<>(nodeNumber);
        Node n = null;

        for(String key : nodes.keySet()){
            n=nodes.get(key);
            if(!n.isFailed()){
                InetSocketAddress inetSocketAddress = n.getSocketAddress();
                aliveNodes.add(inetSocketAddress);
            }
        }

        return aliveNodes;
    }*/

    /*
     * 获取集群中所有故障的节点的套接字地址
     * */
    /*public ArrayList<InetSocketAddress> getFailedNodes(){
        int nodeNumber = nodes.size();
        ArrayList<InetSocketAddress> failedNodes = new ArrayList<>(nodeNumber);
        Node n = null;

        for (String key : nodes.keySet()){
            n = nodes.get(key);
            n.check();
            if(n.isFailed()){
                InetSocketAddress inetSocketAddress = n.getSocketAddress();
                failedNodes.add(inetSocketAddress);
            }
        }

        return failedNodes;
    }*/

    /*public ArrayList<InetSocketAddress> getAllMembers() {

        int initialSize = nodes.size();
        ArrayList<InetSocketAddress> allMembers = new ArrayList<>(initialSize);

        for (String key : nodes.keySet()) {
            Node node = nodes.get(key);
            InetSocketAddress inetSocketAddress = node.getSocketAddress();
            allMembers.add(inetSocketAddress);
        }

        return allMembers;
    }*/

    public void stop(){
        stop = true;
    }



    private void startSendThread(){


        Thread sendThread = new Thread(() -> {
            while (!stop) {
                //GossipRequest currentRequest = sendInfo.getRequestToSend().poll();//取队首并移出队
                sendGossipRequestToOtherNodes();
                try {
                    Thread.sleep(gossipConfig.updateFrequency.toMillis());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "sendThreadMain");
        sendThread.start();
    }

    private void startReceiveThread(){
        Thread receiveThread = new Thread(() -> {
            while (!stop) {
                receiveOtherRequest();
            }
        }, "receiveThread");
        Thread receiveBroadcastThread = new Thread(() -> {
            while (!stop){
                receiveBroadRequest();
            }
        },"receiveBroadcastThread");

        receiveThread.start();
        receiveBroadcastThread.start();
    }

    /*private void startFailureDetectionThread(){
        new Thread(() -> {
            while(!stop){
                detectFailedNodes();
                try{
                    Thread.sleep(gossipConfig.failureDetectionFrequency.toMillis());
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }).start();
    }*/

    private int getRandomIndex(int size){
        Random random = new Random();
        int i = random.nextInt(size);
        return i;
    }

    private boolean detectResponse(String requestID, InetSocketAddress target){
        SendData data = sendDataConcurrentHashMap.get(requestID);

        return data.responseReceived.get(target);
    }


    private void sendGossipRequestToOtherNodes(){

        GossipRequest gossipRequest;
        Object[] currentNodes;

        synchronized (sendInfo) {
            if (sendInfo.structureIsEmpty()) {
                try {
                    sendInfo.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            gossipRequest = sendInfo.getRequestToSend().poll();//取队首并移出队
            currentNodes = sendInfo.getTargets().poll();//取队首并移出队列

            sendInfo.notifyAll();
        }

        List<InetSocketAddress> otherNodes = new ArrayList<>();

        for (int i = 0; i < currentNodes.length; i++) {
            //String key = (String) currentNodes[i];
            InetSocketAddress key = (InetSocketAddress) currentNodes[i];

            if (!key.equals(gossipRequest.getSourceIPAddress())) {
                otherNodes.add(key);
            }
        }

            /*for (int i = 0; i < gossipConfig.maxTransmitNode; i++) {
                //boolean flag = false;
                while (true*//*!flag*//*) {

                    InetSocketAddress key = (InetSocketAddress) currentNodes[getRandomIndex(currentNodes.length)];
                    if (!key.equals(gossipRequest.getSourceIPAddress()) && !otherNodes.contains(key)) {
                        otherNodes.add(key);
                        //flag = true;
                        break;
                    }
                }
            }*/

        /*
         * 对每个需要发送的节点都会开启一个线程进行异步传输
         * */
        try {
            int count = 0;//发送线程计数

            SendData data = new SendData(gossipRequest,otherNodes);

            for (InetSocketAddress target : otherNodes) {
                gossipRequest.setTargetIPAddress(target);

                MyTimer myTimer = new MyTimer(1000,2000, 2);
                myTimer.start(new TimerTask() {
                    @Override
                    public void run() {
                        if (detectResponse(gossipRequest.getRequestID(),target)){
                            data.responseReceived.remove(target);
                            myTimer.stop();
                            data.timers.remove(target);
                        }
                        else {
                            myTimer.count++;
                            if (myTimer.count > myTimer.maxCount){
                                myTimer.count = 0;
                                if (myTimer.maxCount <= 32) {
                                    myTimer.maxCount *= 2;
                                }

                                //重发
                                Thread reSendThread = new Thread(() -> {
                                    GossipRequest request = data.gossipRequest;
                                    request.setTargetIPAddress(target);

                                    try {
                                        socketService.sendGossipRequest(request);
                                    }catch (IOException e){
                                        e.printStackTrace();
                                    }
                                });

                                reSendThread.start();
                            }
                        }
                    }
                });
                data.timers.put(target,myTimer);

                Thread thread = new Thread(() -> {
                        try {
                            long l1 = System.currentTimeMillis();
                            socketService.sendGossipRequest(gossipRequest);
                            long l2 = System.currentTimeMillis();
                            if(Node.isTest()) {
                                System.out.println(Thread.currentThread().getName()+"完成发送的耗时为："+(l2-l1)+"ms");
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }, "sendThread" + count);

                count++;
                System.out.println("当前执行的线程为：" + Thread.currentThread().getName());

                thread.start();
                thread.join();

            }

            data.timer = new MyTimer(2000,1000, 2);
            data.timer.start(new TimerTask() {
                @Override
                public void run() {
                    if (data.responseReceived.size() == 0){
                        data.timer.stop();
                        sendDataConcurrentHashMap.remove(gossipRequest.getRequestID());

                        //int index = sendInfo.getIndexMap().get(gossipRequest.getRequestID());
                        int index = sendInfo.getIndexMap().remove(gossipRequest.getRequestID());

                        synchronized (node.getSendWindow()){
                            HashMap<String,Integer> hashMap = node.getSendWindow().remove(index);//窗口移出一个请求
                            node.getSendWindow().notifyAll();

                            int t;
                            for (String str : hashMap.keySet()){
                                t = hashMap.get(str);
                                sendInfo.getIndexMap().put(str,t);
                            }
                        }

                    }
                }
            });

            sendDataConcurrentHashMap.put(gossipRequest.getRequestID(),data);

        } catch (InterruptedException e) {
                e.printStackTrace();
        }

    }

    public void receiveOtherRequest(){
        Object newData = socketService.receiveData();

        if(newData != null){
            if(newData instanceof GossipRequest) {
                GossipRequest request = (GossipRequest) newData;
                //request.setReceiveTime(System.currentTimeMillis());

                System.out.println(Thread.currentThread().getName() + "：节点" + socketAddress.toString() + "接收到来自节点" + request.getSourceIPAddress().toString() + "的请求");
                //System.out.println(Thread.currentThread().getName() + "：该请求发送和传输所耗费的时间为：" + request.getTransportTimeMillis() + "ms");
                //receivedRequestQueue.add(request);
                synchronized (receiveDataArea.getReceivedRequestQueue()) {

                    if(receiveDataArea.requestQueueFull()){
                        try {
                            receiveDataArea.getReceivedRequestQueue().wait();
                        }catch (InterruptedException e){
                            e.printStackTrace();
                        }

                    }

                    receiveDataArea.getReceivedRequestQueue().add(request);

                    receiveDataArea.getReceivedRequestQueue().notifyAll();
                }
            }
            else if(newData instanceof Response){
                Response response = (Response) newData;
                System.out.println("成功收到来自"+response.getSource()+"的响应");


                synchronized (receiveDataArea.getReceivedResponseQueue()){
                    if (receiveDataArea.responseQueueFull()){
                        try {
                            receiveDataArea.getReceivedResponseQueue().wait();
                        }catch (InterruptedException e){
                            e.printStackTrace();
                        }

                    }

                    receiveDataArea.getReceivedResponseQueue().add(response);
                    receiveDataArea.getReceivedResponseQueue().notifyAll();
                }

            }
        }

        //if(nodes.get(newRequest.getSourceIPAddress().toString()) == null){
        //接收到新的节点发来的gossip请求，维护集群的信息，增加这个新的节点
            /*synchronized (nodes){
                newNode.setGossipConfig(gossipConfig);
                newNode.setLastUpdateTime();
                nodes.putIfAbsent(newNode.getNodeID(),newNode);
                if(onNewMember != null){
                    onNewMember.update(newNode.getSocketAddress());
                }
            }*/
        //nodes.putIfAbsent(newRequest.getSourceIPAddress().toString(),new Node(newRequest.getSourceIPAddress(),0,gossipConfig));
        //}
        //存在异常,existingNode可能为空
        //Node existingNode = nodes.get(newRequest.getSourceIPAddress().toString());
        //System.out.println("节点" + socketAddress.toString() + "成功接收到来自" + newRequest.getSourceIPAddress().toString() + "的向量时钟");
        //解析请求中的向量时钟进行更新
        //currentNode.getVectorClock(newRequest.getKey()).merge(newRequest.getVectorClock());
        //existingNode.updateVersion(newNode.getVersion());


        //接收到请求解析其中的向量时钟并执行算法进行判断是否要更新

    }


    public void receiveBroadRequest(){
        Object newData = socketService.receiveBroadcastData();

        if(newData != null){
            if(newData instanceof GossipRequest) {
                GossipRequest request = (GossipRequest) newData;

                //过滤掉本机自己接收到来自自己的广播请求
                if (request.getSourceIPAddress().equals(socketAddress)){
                    return;
                }

                //request.setReceiveTime(System.currentTimeMillis());
                //Deviation.setRequestReceiveTime(request.receiveTime);
                System.out.println("广播请求接收时刻为："+request.receiveTime);


                System.out.println(Thread.currentThread().getName() + "：节点" + socketAddress.toString() + "接收到来自节点" + request.getSourceIPAddress().toString() + "的广播请求");
                //System.out.println(Thread.currentThread().getName() + "：该广播请求发送和传输所耗费的时间为：" + request.getTransportTimeMillis() + "ms");
                //receivedRequestQueue.add(request);
                synchronized (receiveDataArea.getReceivedRequestQueue()) {

                    if(receiveDataArea.requestQueueFull()){
                        try {
                            receiveDataArea.getReceivedRequestQueue().wait();
                        }catch (InterruptedException e){
                            e.printStackTrace();
                        }

                    }

                    receiveDataArea.getReceivedRequestQueue().add(request);

                    receiveDataArea.getReceivedRequestQueue().notifyAll();
                }
            }
            else if(newData instanceof Response){
                Response response = (Response) newData;

                //System.out.println("成功收到来自"+response.getSource()+"的响应");
                synchronized (receiveDataArea.getReceivedResponseQueue()){
                    if (receiveDataArea.responseQueueFull()){
                        try {
                            receiveDataArea.getReceivedResponseQueue().wait();
                        }catch (InterruptedException e){
                            e.printStackTrace();
                        }

                    }

                    receiveDataArea.getReceivedResponseQueue().add(response);
                    receiveDataArea.getReceivedResponseQueue().notifyAll();
                }

            }
        }


    }

    /*private void detectFailedNodes(){
        String[] keys = new String[nodes.size()];
        nodes.keySet().toArray(keys);

        for (String key : keys){
            Node node = nodes.get(key);
            boolean failed = node.isFailed();
            //存在异常，node可能为空
            node.check();

            //检查后发现不一致
            if(failed != node.isFailed()){
                if(node.isFailed()){
                    if(onFailedMember != null){
                        onFailedMember.update(node.getSocketAddress());
                    }
                    else{
                        if(onRevivedMember != null){
                            onRevivedMember.update(node.getSocketAddress());
                        }
                    }

                }
            }

            if (node.shouldCleanUp()){
                synchronized (nodes){
                    nodes.remove(key);
                    if(onRemovedMember != null){
                        onRemovedMember.update(node.getSocketAddress());
                    }
                }
            }
        }
    }*/

    /*private void printNodes(){
        new Thread(() -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ArrayList<InetSocketAddress> allAliveNodes = getAliveNodes();
            ArrayList<InetSocketAddress> allFailedNodes = getFailedNodes();
            Node node;

            for(InetSocketAddress i : allAliveNodes){
                node = nodes.get(i.toString());
                if(node != null){
                    System.out.println("节点状态: " + node.getHostName() + ":" + node.getPort() + "- 活跃");
                }
            }

            for(InetSocketAddress i : allFailedNodes){
                node = nodes.get(i.toString());
                if(node != null){
                    System.out.println("节点状态: " + node.getHostName() + ":" + node.getPort() + "- 故障");
                }
            }

            *//*getAliveNodes().forEach(node ->
                    System.out.println("节点状态: " + node.getHostName() + ":" + node.getPort() + "- 活跃"));

            getFailedNodes().forEach(node ->
                    System.out.println("节点状态: " + node.getHostName() + ":" + node.getPort() + "- 故障"));*//*
        }).start();
    }*/



}

