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
        request.setSendTime(System.currentTimeMillis());

        byte[] dataReadyToTransport = getDataToTransport(request);

        Node.sendTimeTest.setDataSize(dataReadyToTransport.length);

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

        try{
            ObjectOutput oo = new ObjectOutputStream(byteArrayOutputStream);

            long packageBefore = System.currentTimeMillis();
            oo.writeObject(request);//耗时点
            long packageAfter = System.currentTimeMillis();

            long cost = SendTimeTest.calculate(packageBefore,packageAfter);

            if(cost > Node.sendTimeTest.getWriteObjectMaxTime()) {
                Node.sendTimeTest.setWriteObjectMaxTime(cost);
            }else if(cost < Node.sendTimeTest.getWriteObjectMinTime()){
                Node.sendTimeTest.setWriteObjectMinTime(cost);
            }else{
                Node.sendTimeTest.setWriteObjectTotalTime(Node.sendTimeTest.getWriteObjectTotalTime() + cost);
            }

            //Node.sendTimeTest.setWriteObjectTime(SendTimeTest.calculate(packageBefore,packageAfter));
            //System.out.println("数据流写入所需时间为："+(packageAfter-packageBefore)+"ms");
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
            byte[] data;

            receiveDatagramSocket.receive(receivePacket);

            /*InetAddress a = receivePacket.getAddress();
            int b = receivePacket.getPort();*/
            data = receivePacket.getData();

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);


            //System.out.println("数据流接收所需时间为："+(packageAfter1-packageBefore1)+"ms");

            Object message = null;


            try{


                long packageBefore = System.currentTimeMillis();
                message = objectInputStream.readObject();//耗时点
                long packageAfter = System.currentTimeMillis();
                Node.receiveTimeTest.setReadObjectTime(ReceiveTimeTest.calculate(packageBefore,packageAfter));

                //System.out.println("数据流读取所需时间为："+(packageAfter-packageBefore)+"ms");
                if(message != null){

                }
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
            byte[] data;

            broadcastDatagramSocket.receive(broadcastReceivePacket);

            /*InetAddress a = receivePacket.getAddress();
            int b = receivePacket.getPort();*/

            data = broadcastReceivePacket.getData();

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);


            //System.out.println("数据流接收所需时间为："+(packageAfter1-packageBefore1)+"ms");

            Object message = null;


            try{


                long packageBefore = System.currentTimeMillis();
                message = objectInputStream.readObject();//耗时点
                long packageAfter = System.currentTimeMillis();
                Node.receiveTimeTest.setReadObjectTime(ReceiveTimeTest.calculate(packageBefore,packageAfter));

                //System.out.println("数据流读取所需时间为："+(packageAfter-packageBefore)+"ms");
                if(message != null){

                }
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