package drz.tmdb.map;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import drz.tmdb.memory.Tuple;
import drz.tmdb.Transaction.Transactions.Create;
import drz.tmdb.Transaction.Transactions.Insert;
import drz.tmdb.Transaction.Transactions.Select;
import drz.tmdb.Transaction.Transactions.impl.CreateImpl;
import drz.tmdb.Transaction.Transactions.impl.InsertImpl;
import drz.tmdb.Transaction.Transactions.impl.SelectImpl;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;
import drz.tmdb.Transaction.Transactions.utils.SelectResult;

public class TrajectoryUtils {

    private static MemConnect memConnect;

    public TrajectoryUtils(MemConnect memConnect){
        this.memConnect = memConnect;

        // 初始化两张轨迹表
        init();
    }

    // 初始化两张轨迹表
    public void init(){
        try{
            String sql1 = "CREATE CLASS mobile_phone_traj (trajectory_id int,user_id char, trajectory char);";
            String sql2 = "CREATE CLASS watch_traj (trajectory_id int,user_id char, trajectory char);";
            Create create = new CreateImpl(memConnect);
            Statement parse1 = CCJSqlParserUtil.parse(sql1);
            Statement parse2 = CCJSqlParserUtil.parse(sql2);
            create.create(parse1);
            create.create(parse2);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    // 持久化轨迹
    public static void save(ArrayList<TrajectoryPoint> trajectory){
        if(trajectory == null || trajectory.size() == 0)
            return;
        int tID = getTrajectoryID();
        String uID = trajectory.get(0).userId;
        String tString = serialize(trajectory); // 序列化后的位置信息

        // 构造SQL语句
        String sql;
        if(tID % 2 == 1){
            sql = String.format("INSERT INTO mobile_phone_traj VALUES (%d,%s,%s);", tID, uID, tString);
        }else{
            sql = String.format("INSERT INTO watch_traj VALUES (%d,%s,%s);", tID, uID, tString);
        }

        // 执行SQl语句
        try {
            Insert insert = new InsertImpl(memConnect);
            Statement parse = CCJSqlParserUtil.parse(sql);
            insert.insert(parse);
        }catch (Throwable e){
            e.printStackTrace();
        }

    }

    // 读取历史轨迹数据，每条轨迹是若干个轨迹点组成的ArrayList
    public static ArrayList<ArrayList<TrajectoryPoint>> load(){
        ArrayList<ArrayList<TrajectoryPoint>> ret = new ArrayList<ArrayList<TrajectoryPoint>>();
        String sql1 = "SELECT * FROM mobile_phone_traj;";
        String sql2 = "SELECT * FROM watch_traj;";
        try{
            Select select = new SelectImpl(memConnect);
            Statement parse1 = CCJSqlParserUtil.parse(sql1);
            SelectResult result1 = select.select(parse1);
            Statement parse2 = CCJSqlParserUtil.parse(sql2);
            SelectResult result2 = select.select(parse2);
            for(Tuple t: result1.getTpl().tuplelist){
                ret.add(deserialize((String) t.tuple[2]));
            }
            for(Tuple t: result2.getTpl().tuplelist){
                ret.add(deserialize((String) t.tuple[2]));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return ret;
    }


    // 获取TrajectoryID
    public static int getTrajectoryID(){
        int tid = -1;
        try{
            File f = new File("/data/data/drz.tmdb/tid");
            // 首次创建则tid为1
            if(!f.exists()){
                f.createNewFile();
                RandomAccessFile raf = new RandomAccessFile(f, "rw");
                raf.writeInt(1);
                tid = 1;
            }
            // 每次tid递增1
            else{
                RandomAccessFile raf = new RandomAccessFile(f, "rw");
                tid = raf.readInt() + 1;
                raf.seek(0);
                raf.writeInt(tid);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return tid;
    }


    // 将轨迹序列化
    public static String serialize(ArrayList<TrajectoryPoint> trajectory){
        if(trajectory == null || trajectory.size() == 0)
            return "";
        StringBuilder ret = new StringBuilder();
        for(TrajectoryPoint point : trajectory){
            ret.append(point.longitude);
            ret.append("-");
            ret.append(point.latitude);
            ret.append("-");
        }
        return ret.toString().substring(0, ret.length()-1);
//        if(trajectory == null || trajectory.size() == 0)
//            return "";
//        StringBuilder ret = new StringBuilder();
//        for(TrajectoryPoint point : trajectory){
//            ret.append(new String(double2Bytes(point.longitude)));
//            ret.append(new String(double2Bytes(point.latitude)));
//        }
//        return ret.toString();
    }

    // 将String反序列化成轨迹
    public static ArrayList<TrajectoryPoint> deserialize(String str){
        ArrayList<TrajectoryPoint> ret = new ArrayList<>();
        String[] info = str.split("-");
        int pointCount = info.length / 2;
        for(int i=0; i<pointCount; i++){
            ret.add(new TrajectoryPoint(Double.parseDouble(info[i]), Double.parseDouble(info[i+1])));
        }
        return ret;
//        ArrayList<TrajectoryPoint> ret = new ArrayList<>();
//        int pointCount = str.length() / 16;
//        for(int i=0; i<pointCount; i++){
//            byte[] lo = str.substring(8 * i, 8 * i + 8).getBytes();
//            byte[] la = str.substring(8 * i + 8, 8 * i + 16).getBytes();
//            ret.add(new TrajectoryPoint(bytes2Double(lo), bytes2Double(la)));
//        }
//        return ret;
    }

    private static byte[] double2Bytes(double d) {
        byte[] data = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try{
            dos.writeDouble(d);
            dos.flush();
            data = bos.toByteArray();
            dos.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return data;
    }

    public static double bytes2Double(byte[] arr){
        double num = 0;
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(arr));
        try{
            num = dis.readDouble();
            dis.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return num;
    }

}
