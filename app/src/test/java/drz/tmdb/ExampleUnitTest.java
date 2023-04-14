package drz.tmdb;

import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

import drz.tmdb.sync.Sync;
import drz.tmdb.sync.config.GossipConfig;
import drz.tmdb.sync.network.GossipRequest;
import drz.tmdb.sync.network.SocketService;
import drz.tmdb.sync.node.Node;
import drz.tmdb.sync.node.database.Action;
import drz.tmdb.sync.node.database.OperationType;
import drz.tmdb.sync.persist.Persist;
import drz.tmdb.sync.share.RequestType;
import drz.tmdb.sync.statistics.WriteCSV;
import drz.tmdb.sync.timeTest.SendTimeTest;
import drz.tmdb.sync.util.NetworkUtil;
import drz.tmdb.sync.util.NodeUtil;
import drz.tmdb.sync.vectorClock.ClockEntry;
import drz.tmdb.sync.vectorClock.ClockEntryHeap;
import drz.tmdb.sync.vectorClock.Relation;
import drz.tmdb.sync.vectorClock.VectorClock;


/**
 * Example local unit test, which will execute on the development machine (host).
 *
 * @see <a href="http://d.android.com/tools/testing">Testing documentation</a>
 */
public class ExampleUnitTest {
    @Test
    public void addition_isCorrect() throws IOException,InterruptedException {
        assertEquals(4, 2 + 2);
        GossipConfig gossipConfig = new GossipConfig(
                Duration.ofSeconds(3),
                Duration.ofSeconds(3),
                Duration.ofMillis(10),
                Duration.ofMillis(10),
                Duration.ofMillis(5000),
                2,
                2,
                1,
                2,
                65536,
                10
        );

        ArrayList<InetSocketAddress> nodeCluster = new ArrayList<>();

        String nodeSelf = InetAddress.getLocalHost().getHostAddress();
        InetAddress ip = InetAddress.getByName(nodeSelf);
        String nodeID = NodeUtil.obtainNodeID();
        Node initialNode = new Node(nodeID, ip, 9090, gossipConfig);
        nodeCluster.add(initialNode.getSocketAddress());
        initialNode.start();


        final Long key = 1L;



        //initialNode.getGossipController().start();


        for (int i = 1; i <= 5; i++) {
            /*GossipController gossipService = new GossipController
                    (new InetSocketAddress("127.0.0.1", 9090 + i),
                            new InetSocketAddress("127.0.0.1", 9090 + i - 1), gossipConfig);
            gossipService.start(true,initialNode.generateGossipRequest(key));*/
            //InetAddress otherIP = InetAddress.getByName(nodeSelf);
            String otherNodeID = NodeUtil.obtainNodeID();
            Node otherNode = new Node(otherNodeID, ip,9090+i, gossipConfig);
            nodeCluster.add(otherNode.getSocketAddress());
            otherNode.start();
            /*initialNode.getGossipController().getNodes().putIfAbsent(otherNode.getNodeID(), otherNode);
            otherNode.getGossipController().getNodes().putIfAbsent(initialNode.getNodeID(), initialNode);*/
            //otherNode.getGossipController().start();
        }

        //initialNode.broadcast1(nodeCluster);

        //insert into test values("a",1,10.0);
        Action action = new Action(
                OperationType.insert,
                "test",
                "test",
                1,
                3,
                new String[]{"name", "age", "num"},
                new String[]{"String", "Integer", "Double"},
                new String[]{"a", "1", "10.0"});

        /*for (int m = 1; m <= 3; m++) {

            for (int n = 1; n <= m ; n++){
                //initialNode.updateVectorClock(key);
                initialNode.getDataManager().putAction(action);
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            SendTimeTest s = Node.sendTimeTest.get(Node.batch_num);
            s.getProcessQueueAverageTime();
            s.getSendRequestAverageTime();
            s.getWriteObjectAverageTime();
            Node.batch_num++;

        }*/



        Thread.sleep(10000);

        ArrayList<String[]> sendHead = WriteCSV.getAttributes(new SendTimeTest());
        ArrayList<String[]> sendDataList = new ArrayList<>();
        SendTimeTest test;
        String[] tmp;

        /*for (Integer i : Node.sendTimeTest.keySet()){
            test = Node.sendTimeTest.get(i);
            tmp = WriteCSV.getAttributeValue(test);
            sendDataList.add(tmp);
        }*/

        WriteCSV.writeCSVFile("sendTime3",sendHead,sendDataList,false);
        /*ArrayList<String[]> receiveHead = WriteCSV.getAttributes(Node.receiveTimeTest);
        String[] receiveData = WriteCSV.getAttributeValue(Node.receiveTimeTest);
        ArrayList<String[]> receiveDataList = new ArrayList<>();
        receiveDataList.add(receiveData);
        WriteCSV.writeCSVFile("receiveTime",receiveHead,receiveDataList,false);*/
        //while(true){}
        //ArrayList<String[]> data = WriteCSV.getData(new TimeTest());
        //WriteCSV.writeCSVFile("test",data);
    }

    @Test
    public void testBroadcast() throws Exception {
        GossipConfig gossipConfig = new GossipConfig(
                Duration.ofSeconds(3),
                Duration.ofSeconds(3),
                Duration.ofMillis(10),
                Duration.ofMillis(10),
                Duration.ofMillis(5000),
                3,
                2,
                1,
                2,
                65536,
                10
        );


        //String nodeSelf = InetAddress.getLocalHost().getHostAddress();
        String nodeSelf = NetworkUtil.getLocalHostIP();
        InetAddress ip = InetAddress.getByName(nodeSelf);
        System.out.println("IP地址为：" + ip);

        String nodeID = NodeUtil.obtainNodeID();
        Node initialNode = new Node(nodeID, ip, 9091, gossipConfig);

        initialNode.start();



        //initialNode.broadcast();
        try {
            Thread.sleep(10000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        while (true){}


    }

    @Test
    public void testUUID(){
        System.out.println(NodeUtil.obtainNodeID());
    }

    @Test
    public void testCompare(){

        VectorClock v1 = new VectorClock();
        VectorClock v2 = new VectorClock();


        System.out.println("向量时钟v1所占内存大小为："+RamUsageEstimator.sizeOf(v1));
        System.out.println("向量时钟v2所占内存大小为："+RamUsageEstimator.sizeOf(v2));
        /*for (int i = 0 ;i<3;i++){
            v1.increaseVersion("A",System.currentTimeMillis());
            v1.increaseVersion("B",System.currentTimeMillis());
            v1.increaseVersion("C",System.currentTimeMillis());
        }

        for (int i = 0 ;i<2;i++){
            v2.increaseVersion("A",System.currentTimeMillis());
            v2.increaseVersion("B",System.currentTimeMillis());
        }*/

        for (int i = 0 ;i<1;i++){
            //v1.put("AVB",1);
            v1.put("BESA",6);
            //v1.put("SFC",7);
            v1.put("DVBFZCF",9);
            v1.put("EBZDB",2);
            //v1.put("FMHJSGRW",3);
            v1.put("GHTJMZDFG",4);
            v1.put("Hocwdjkoh",1);
           /* v1.put("Ioydmg",8);
            v1.put("JDHVDANM",3);
            v1.put("KJLDWFVBHF",4);*/
            v1.put("LSAAQWGBNMTR",4);
            v1.put("MHFDSAADR",9);
            v1.put("NGGDSSAAGAH",8);
            v1.put("OAHAYRNCX",6);
            v1.put("PVFNMF",3);
            //v1.put("QNYIOPERGH",7);
            v1.put("RVBAXZYUEW",1);
            v1.put("SLMYZLUAJXYA",4);
            v1.put("DUNXISJAZK",8);
            //v1.put("UXAQXCRAZS",1);
            v1.put("VFFTBXZFGH",6);
           // v1.put("WLODWNXUAJKZM",2);
            v1.put("XXSFIHDSA",2);
            v1.put("Y",3);
        }


        System.out.println("向量时钟v1所占内存大小为："+RamUsageEstimator.sizeOf(v1));
        System.out.println("向量时钟v2所占内存大小为："+RamUsageEstimator.sizeOf(v2));
        Relation r;
        do {
            r = VectorClock.compare(v1,v2);
            System.out.println(r.toString());

            try {
                Thread.sleep(5000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }

        }while (true);

    }

    @Test
    public void test() throws Exception {
        Sync.initialNode(9090);


        Action action = new Action(
                OperationType.insert,
                "test",
                "test",
                1,
                3,
                new String[]{"name", "age", "number"},
                new String[]{"String", "Integer", "Double"},
                new String[]{"a", "1", "10.0"});


        while (true){
            Sync.syncStart(action);
            try {
                Thread.sleep(6000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }


    @Test
    public void testActionGenerate(){
        //String sql ="insert into test (name,age,number) values(\"a\",1,10.0)";
        //String sql = "select name from test where age=1 or number = 10.0";
        //String sql = "delete from test where age = 12 ";
        String sql = " update test set name=\"stu\", age=2 where tel=\"13288097888\" ";
        ArrayList<Long> keys = new ArrayList<>();
        for (int i = 0 ;i<5;i++){
            keys.add(Long.parseLong(Integer.toString(i)));
        }
        Action.generate("",sql,keys);

    }

    @Test
    public void testMMAP() throws Exception{

        Sync.initialNode(9090);


        try {
            Thread.sleep(10000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        Node node = Sync.getNode();
        Sync.initialNode(9091);

        try {
            Thread.sleep(10000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        Node node1 = Sync.getNode();

        while (true){}

    }

    @Test
    public void testHeap(){
        /*ClockEntry entry = new ClockEntry("A");

        ClockEntryHeap heap = new ClockEntryHeap(100,10);
        String[] str = new String[]{"A","B","C","D","E","F","G","H","I"};

        for (int i = 0 ;i<str.length;i++){
            heap.put(str[i]);
            try {
                Thread.sleep(5);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }

        heap.put("B");

        try {
            Thread.sleep(5);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        System.out.println("123");*/


    }
}