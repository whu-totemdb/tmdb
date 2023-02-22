package drz.tmdb.sync.network;

import java.io.*;
import java.net.*;

import drz.tmdb.sync.node.Node;

import drz.tmdb.sync.share.RequestType;
import drz.tmdb.sync.share.ResponseType;
import drz.tmdb.sync.timeTest.ReceiveTimeTest;
import drz.tmdb.sync.timeTest.SendTimeTest;
import drz.tmdb.sync.timeTest.TimeTest;


public class SocketService {

    private DatagramSocket receiveDatagramSocket;

    private DatagramSocket broadcastDatagramSocket;



    private int receivePort;//接收端口号

    private final int bufferSize = 65536;//64KB接收区大小

    private byte[] receivedBuffer = new byte[bufferSize];//接收区

    private byte[] broadcastReceivedBuffer = new byte[bufferSize];//广播信息接收区

    private DatagramPacket receivePacket = new DatagramPacket(receivedBuffer,receivedBuffer.length);

    private DatagramPacket broadcastReceivePacket = new DatagramPacket(broadcastReceivedBuffer,broadcastReceivedBuffer.length);

    public SocketService(int receivePort){
        try{

            this.receivePort = receivePort;
            receiveDatagramSocket = new DatagramSocket(receivePort);
            broadcastDatagramSocket = new DatagramSocket(Node.broadcastPort);

        }catch (SocketException exception){
            System.out.println("建立连接失败！");
            exception.printStackTrace();
        }
    }




    public void sendGossipRequest(/*Node source,Node target,*/GossipRequest request) throws IOException{

        int batch_id = request.batch_id;

        //request.setSendTime(System.currentTimeMillis());

        //byte[] dataReadyToTransport = getDataToTransport(request);

        /*if(!Node.batch_id_map.get(batch_id).contains(request.getRequestID())) {//如果是第一次处理此id号的请求
            Node.batch_id_map.get(batch_id).add(request.getRequestID());
            Node.sendTimeTest.get(batch_id).setDataSize(dataReadyToTransport.length);

        }

        System.out.println(Thread.currentThread().getName() + "：发送的请求大小为：" + dataReadyToTransport.length + "B");*/

        System.out.println(
                Thread.currentThread().getName()
                + "：节点"
                + request.getSourceIPAddress().getAddress().toString()
                + "向节点"
                + request.getTargetIPAddress().getAddress().toString()
                + "发送数据");

        sendToTargetNode(request.getTargetIPAddress(),request);
    }



    public static byte[] getDataToTransport(Object message){
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        //统计使用
        /*boolean isRequest = false;
        int batch_id;
        if (request instanceof GossipRequest) {
            GossipRequest gRequest = (GossipRequest) request;
            batch_id = gRequest.batch_id;
            isRequest = true;
        }else {
            batch_id = 0;
        }*/

        try{
            ObjectOutput oo = new ObjectOutputStream(byteArrayOutputStream);
            if (message instanceof Response && ((Response) message).getResponseType() == ResponseType.broadcastResponse){
                ((Response) message).setSendTime(System.currentTimeMillis());
                System.out.println(((Response) message).getSource()+"的广播响应发送时刻为："+System.currentTimeMillis());
            }

            oo.writeObject(message);//耗时点

            //统计使用
            /*if (isRequest) {
                SendTimeTest sendTimeTest = Node.sendTimeTest.get(batch_id);

                if (sendTimeTest != null) {
                    sendTimeTest.setWriteObjectTimeOnce(cost);

                    if (cost > sendTimeTest.getWriteObjectMaxTime()) {
                        sendTimeTest.setWriteObjectMaxTime(cost);

                    } else if (cost < sendTimeTest.getWriteObjectMinTime()) {
                        sendTimeTest.setWriteObjectMinTime(cost);
                    }
                }
            }*/


            oo.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        return byteArrayOutputStream.toByteArray();
    }

    public void sendToTargetNode(InetSocketAddress target , Object message) throws IOException{

        ServerSocket serverSocket = new ServerSocket(0);
        int sendPort;
        do {
            sendPort = serverSocket.getLocalPort();
        }while (sendPort == receivePort || sendPort == Node.broadcastPort);
        DatagramSocket sendDatagramSocket = new DatagramSocket(sendPort);
        long l1 = System.currentTimeMillis();
        byte[] data = getDataToTransport(message);//序列化
        long l2 = System.currentTimeMillis();
        System.out.println(Thread.currentThread().getName()+"：序列化耗时为"+(l2-l1)+"ms");
        if(Node.isTest()) {
            if(message instanceof GossipRequest) {
                TimeTest.putInCostTreeMap(((GossipRequest) message).getRequestID(), Thread.currentThread().getName()+"序列化耗时", (l2 - l1));
            }
        }

        DatagramPacket packet = new DatagramPacket(data,data.length,target.getAddress(), target.getPort());

        try{

            sendDatagramSocket.send(packet);
        }catch (IOException e){

            e.printStackTrace();
        }
    }

    public static void broadcast(InetAddress address, GossipRequest gossipRequest) throws IOException{

        ServerSocket serverSocket = new ServerSocket(0);
        int sendPort;
        do {
            sendPort = serverSocket.getLocalPort();
        }while (sendPort == Node.broadcastPort);
        DatagramSocket sendDatagramSocket = new DatagramSocket(sendPort);

        byte[] data = getDataToTransport(gossipRequest);//序列化
        DatagramPacket packet = new DatagramPacket(data,data.length,address, Node.broadcastPort);

        try{
            sendDatagramSocket.send(packet);
            Deviation.setRequestSendTime(System.currentTimeMillis());//记录请求发送完的时刻
            System.out.println("广播请求发送时刻为："+System.currentTimeMillis());
            System.out.println(Thread.currentThread().getName() + "成功向本子网内其他节点发送广播请求");
        }catch (IOException e){

            e.printStackTrace();
        }
    }

    public Object receiveData(){
        try{
            receiveDatagramSocket.receive(receivePacket);

            /*InetAddress a = receivePacket.getAddress();
            int b = receivePacket.getPort();*/
            byte[] data = receivePacket.getData();

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);

            Object message = null;

            try{
                long packageBefore = System.currentTimeMillis();
                message = objectInputStream.readObject();//耗时点
                long packageAfter = System.currentTimeMillis();
                System.out.println(Thread.currentThread().getName()+"：反序列化耗时："+(packageAfter - packageBefore)+"ms");

                if (message instanceof Response){
                    Response response = (Response) message;
                    if (response.getResponseType() == ResponseType.broadcastResponse){
                        Deviation.setResponseReceiveTime(System.currentTimeMillis());
                        System.out.println("广播响应接收时刻为："+System.currentTimeMillis());
                    }
                    else {
                        if(Node.isTest()) {
                            TimeTest.putInCostTreeMap(response.getRequestID(),"响应反序列化耗时",(packageAfter - packageBefore));
                        }
                    }
                }
                else if(message instanceof GossipRequest){
                    GossipRequest request = (GossipRequest) message;

                    String id = request.getRequestID();

                    if(Node.isTest()) {
                        TimeTest.putInCostTreeMap(request.getRequestID(),"请求反序列化耗时",(packageAfter - packageBefore));
                    }
                    /*if (!Node.receiveTimeTest.containsKey(id)){
                        ReceiveTimeTest r = new ReceiveTimeTest();
                        r.setReadObjectTime(ReceiveTimeTest.calculate(packageBefore,packageAfter));
                        Node.receiveTimeTest.put(id,r);
                    }*/
                }


                //System.out.println("数据流读取所需时间为："+(packageAfter-packageBefore)+"ms");

            }catch (ClassNotFoundException classNotFoundException){
                classNotFoundException.printStackTrace();
            }finally {
                objectInputStream.close();
                return message;
            }

        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }

    public Object receiveBroadcastData(){
        try{

            broadcastDatagramSocket.receive(broadcastReceivePacket);
            byte[] data = broadcastReceivePacket.getData();

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);

            Object message = null;

            try{
                long packageBefore = System.currentTimeMillis();
                message = objectInputStream.readObject();//耗时点
                long packageAfter = System.currentTimeMillis();
                System.out.println("反序列化耗时："+(packageAfter - packageBefore)+"ms");


            }catch (ClassNotFoundException classNotFoundException){
                classNotFoundException.printStackTrace();
            }finally {
                objectInputStream.close();
                return message;
            }

        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }



}