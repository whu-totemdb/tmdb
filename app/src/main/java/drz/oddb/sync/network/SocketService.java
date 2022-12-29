package drz.oddb.sync.network;

import java.io.*;
import java.net.*;

import drz.oddb.sync.timeTest.TimeTest;

public class SocketService {
    private DatagramSocket sendDatagramSocket;

    private DatagramSocket receiveDatagramSocket;

    private int receivePort;//接收端口号

    private final int bufferSize = 65536;//64KB接收区大小

    private byte[] receivedBuffer = new byte[bufferSize];//接收区

    private DatagramPacket receivePacket = new DatagramPacket(receivedBuffer,receivedBuffer.length);

    public SocketService(int receivePort){
        try{

            this.receivePort = receivePort;
            receiveDatagramSocket = new DatagramSocket(receivePort);

        }catch (SocketException exception){
            System.out.println("建立连接失败！");
            exception.printStackTrace();
        }
    }

    public void sendGossipRequest(/*Node source,Node target,*/GossipRequest request) throws IOException{

        byte[] dataReadyToTransport = getDataToTransport(request);

        sendToTargetNode(request.getSourceIPAddress(),request.getTargetIPAddress(),dataReadyToTransport);
    }



    private byte[] getDataToTransport(GossipRequest request){
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        System.out.println(Thread.currentThread().getName() + "：节点" + request.getSourceIPAddress().toString() + "正在向缓存区写入待传输的数据");
        try{
            ObjectOutput oo = new ObjectOutputStream(byteArrayOutputStream);
            request.setSendTime(System.currentTimeMillis());
            long packageBefore = System.currentTimeMillis();
            oo.writeObject(request);//耗时点
            long packageAfter = System.currentTimeMillis();
            TimeTest.setWriteObjectTime(TimeTest.calculate(packageBefore,packageAfter));
            //System.out.println("数据流写入所需时间为："+(packageAfter-packageBefore)+"ms");
            oo.close();
        }catch (IOException e){
            System.out.println(Thread.currentThread().getName() + "：由于" + e.getMessage() + "节点" + request.getSourceIPAddress().toString() + "发送失败...");
            e.printStackTrace();
        }
        return byteArrayOutputStream.toByteArray();
    }

    private void sendToTargetNode(InetAddress source, InetSocketAddress target , byte[] data) throws IOException{

        DatagramPacket packet=new DatagramPacket(data,data.length,target.getAddress(), target.getPort());


        ServerSocket serverSocket = new ServerSocket(0);
        int sendPort;
        do {
            sendPort = serverSocket.getLocalPort();
        }while (sendPort == receivePort);
        sendDatagramSocket = new DatagramSocket(sendPort);


        try{
            System.out.println(Thread.currentThread().getName() + "：节点"+ source.toString() + "向节点" + target.toString() + "发送数据");
            sendDatagramSocket.send(packet);
        }catch (IOException e){
            System.out.println(Thread.currentThread().getName() + "：节点"+ source.toString() + "向节点" + target.toString() + "发送数据失败");
            e.printStackTrace();
        }
    }

    /*
     * 待修改为接受gossip请求
     * */
    public GossipRequest receiveGossipRequest(){
        try{

            receiveDatagramSocket.receive(receivePacket);


            byte[] data = receivePacket.getData();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
            long packageBefore1 = System.currentTimeMillis();
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);//耗时点
            long packageAfter1 = System.currentTimeMillis();
            TimeTest.setObjectInputStreamInitialTime(TimeTest.calculate(packageBefore1,packageAfter1));
            //System.out.println("数据流接收所需时间为："+(packageAfter1-packageBefore1)+"ms");

            GossipRequest message = null;


            try{
                long packageBefore = System.currentTimeMillis();
                message = (GossipRequest) objectInputStream.readObject();//耗时点
                long packageAfter = System.currentTimeMillis();
                TimeTest.setReadObjectTime(TimeTest.calculate(packageBefore,packageAfter));
                //System.out.println("数据流读取所需时间为："+(packageAfter-packageBefore)+"ms");
                if(message != null){
                    message.setReceiveTime(System.currentTimeMillis());
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