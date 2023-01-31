package drz.oddb.sync.network;

import android.util.Log;

import java.io.*;
import java.net.*;
import java.util.ArrayList;

import drz.oddb.sync.node.Node;

import drz.oddb.sync.timeTest.ReceiveTimeTest;
import drz.oddb.sync.timeTest.SendTimeTest;


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

        request.setSendTime(System.currentTimeMillis());

        byte[] dataReadyToTransport = getDataToTransport(request);

        if(!Node.batch_id_map.get(batch_id).contains(request.getRequestID())) {//如果是第一次处理此id号的请求
            Node.batch_id_map.get(batch_id).add(request.getRequestID());
            Node.sendTimeTest.get(batch_id).setDataSize(dataReadyToTransport.length);

        }

        System.out.println(Thread.currentThread().getName() + "：发送的请求大小为：" + dataReadyToTransport.length + "B");

        System.out.println(
                Thread.currentThread().getName()
                + "：节点"
                + request.getSourceIPAddress().getAddress().toString()
                + "向节点"
                + request.getTargetIPAddress().getAddress().toString()
                + "发送数据");

        sendToTargetNode(request.getTargetIPAddress(),dataReadyToTransport);
    }



    public static byte[] getDataToTransport(Object request){
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        //统计使用
        boolean isRequest = false;
        int batch_id;
        if (request instanceof GossipRequest) {
            GossipRequest gRequest = (GossipRequest) request;
            batch_id = gRequest.batch_id;
            isRequest = true;
        }else {
            batch_id = 0;
        }




        try{
            ObjectOutput oo = new ObjectOutputStream(byteArrayOutputStream);

            long packageBefore = System.currentTimeMillis();
            oo.writeObject(request);//耗时点
            long packageAfter = System.currentTimeMillis();

            long cost = SendTimeTest.calculate(packageBefore,packageAfter);

            //统计使用
            if (isRequest) {
                SendTimeTest sendTimeTest = Node.sendTimeTest.get(batch_id);

                if (sendTimeTest != null) {
                    sendTimeTest.setWriteObjectTimeOnce(cost);

                    if (cost > sendTimeTest.getWriteObjectMaxTime()) {
                        sendTimeTest.setWriteObjectMaxTime(cost);

                    } else if (cost < sendTimeTest.getWriteObjectMinTime()) {
                        sendTimeTest.setWriteObjectMinTime(cost);
                    }
                }
            }


            oo.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        return byteArrayOutputStream.toByteArray();
    }

    public void sendToTargetNode(InetSocketAddress target , byte[] data) throws IOException{

        DatagramPacket packet = new DatagramPacket(data,data.length,target.getAddress(), target.getPort());

        ServerSocket serverSocket = new ServerSocket(0);
        int sendPort;
        do {
            sendPort = serverSocket.getLocalPort();
        }while (sendPort == receivePort || sendPort == Node.broadcastPort);
        DatagramSocket sendDatagramSocket = new DatagramSocket(sendPort);


        try{

            sendDatagramSocket.send(packet);
        }catch (IOException e){

            e.printStackTrace();
        }
    }

    public static void broadcast(InetAddress address, byte[] data) throws IOException{
        DatagramPacket packet = new DatagramPacket(data,data.length,address, Node.broadcastPort);

        ServerSocket serverSocket = new ServerSocket(0);
        int sendPort;
        do {
            sendPort = serverSocket.getLocalPort();
        }while (sendPort == Node.broadcastPort);
        DatagramSocket sendDatagramSocket = new DatagramSocket(sendPort);


        try{
            sendDatagramSocket.send(packet);
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

                if (message instanceof GossipRequest){
                    GossipRequest request = (GossipRequest) message;
                    int id = request.getRequestID();

                    if (!Node.receiveTimeTest.containsKey(id)){
                        ReceiveTimeTest r = new ReceiveTimeTest();
                        r.setReadObjectTime(ReceiveTimeTest.calculate(packageBefore,packageAfter));
                        Node.receiveTimeTest.put(id,r);
                    }
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
                message = objectInputStream.readObject();

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