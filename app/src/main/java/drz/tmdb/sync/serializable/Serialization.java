package drz.tmdb.sync.serializable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import drz.tmdb.sync.network.GossipRequest;
import drz.tmdb.sync.network.Response;
import drz.tmdb.sync.node.Node;
import drz.tmdb.sync.share.ResponseType;

public class Serialization {

    public static byte[] serialization(Object message){
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
            if(Node.isTest()){
                if (message instanceof GossipRequest){
                    ((GossipRequest) message).sendTime = System.currentTimeMillis();
                }
                else if(message instanceof Response){
                    if (((Response) message).getResponseType() != ResponseType.broadcastResponse) {
                        ((Response) message).requestProcessTime += System.currentTimeMillis();
                    }
                }
            }

            if (message instanceof Response){
                ((Response) message).setSendTime(System.currentTimeMillis());//设置响应的发送时刻
                if (((Response) message).getResponseType() == ResponseType.broadcastResponse) {
                    System.out.println(((Response) message).getSource() + "的广播响应发送时刻为：" + System.currentTimeMillis());
                }

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


    public static Object deSerialization(byte[] data) throws
            IOException,
            ClassNotFoundException{

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);

        Object message = null;

        message = objectInputStream.readObject();//耗时点

        objectInputStream.close();
        return message;

    }
}
