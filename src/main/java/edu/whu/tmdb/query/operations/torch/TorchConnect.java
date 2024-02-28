package edu.whu.tmdb.query.operations.torch;


import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.operations.torch.proto.IdEdge;
import edu.whu.tmdb.query.operations.torch.proto.IdEdgeRaw;
import edu.whu.tmdb.query.operations.torch.proto.IdVertex;
import edu.whu.tmdb.query.operations.torch.proto.TrajectoryTimePartial;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import au.edu.rmit.bdm.Test;
import au.edu.rmit.bdm.Torch.base.Torch;
import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import au.edu.rmit.bdm.Torch.queryEngine.Engine;
import au.edu.rmit.bdm.Torch.queryEngine.model.SearchWindow;
import au.edu.rmit.bdm.Torch.queryEngine.query.QueryResult;
import edu.whu.tmdb.query.operations.Create;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Insert;
import edu.whu.tmdb.query.operations.Select;
import edu.whu.tmdb.query.operations.impl.CreateImpl;
import edu.whu.tmdb.query.operations.impl.InsertImpl;
import edu.whu.tmdb.query.operations.impl.SelectImpl;
import edu.whu.tmdb.query.operations.utils.Constants;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

public class TorchConnect {

    private static Logger logger = LoggerFactory.getLogger(TorchConnect.class);
    static TorchConnect torchConnect;
    static Engine engine;
    String baseDir;
    MemConnect memConnect;

//    TorchSQLiteHelper helper;


    public TorchConnect(MemConnect memConnect, String baseDir){
        this.baseDir=Constants.TORCH_RES_BASE_DIR+"/"+baseDir;
        this.memConnect=memConnect;
//        this.helper=new TorchSQLiteHelper(this.baseDir+"/Torch/db/"+baseDir+".db");
    }

    
    public void testSQLiteHelper(){
//        File databasePath = context.getDatabasePath("data.db");
        String attr="id";
        String sql = "SELECT content from " + "edge" + " WHERE "+attr+ " = ?";
        String selection = attr+" = ?";
        String[] selectionArgs = {"1"};
    }

    public static void init(MemConnect memConnect, String baseDir){
        torchConnect=new TorchConnect(memConnect, baseDir);
    }

    public static TorchConnect getTorchConnect(){
        return torchConnect;
    }

    public void test() throws IOException {
//        String lines="1\t[[-8.639847,41.159826],[-8.640351,41.159871],[-8.642196,41.160114],[-8.644455,41.160492],[-8.646921,41.160951],[-8.649999,41.161491],[-8.653167,41.162031],[-8.656434,41.16258],[-8.660178,41.163192],[-8.663112,41.163687],[-8.666235,41.1642],[-8.669169,41.164704],[-8.670852,41.165136],[-8.670942,41.166576],[-8.66961,41.167962],[-8.668098,41.168988],[-8.66664,41.170005],[-8.665767,41.170635],[-8.66574,41.170671]]\n" +
//                "55\t[[-8.630568,41.154795],[-8.63064,41.154813],[-8.631495,41.1543],[-8.632521,41.152905],[-8.632539,41.152815],[-8.633241,41.152599],[-8.63586,41.152428],[-8.637237,41.152761],[-8.637264,41.152788],[-8.638929,41.153166],[-8.641692,41.15385],[-8.644383,41.154489],[-8.646048,41.153985],[-8.645634,41.153301],[-8.645418,41.153148],[-8.645391,41.15223],[-8.645454,41.152122],[-8.645436,41.152131],[-8.645355,41.152284],[-8.645652,41.153346],[-8.646075,41.15412],[-8.64711,41.154264],[-8.648082,41.154615],[-8.64855,41.156397],[-8.649513,41.158791],[-8.648766,41.15934],[-8.650287,41.1606],[-8.650242,41.161311],[-8.649027,41.161185],[-8.646804,41.160861],[-8.646786,41.160861],[-8.646399,41.161203],[-8.645247,41.161464],[-8.643897,41.161257],[-8.643753,41.161995],[-8.64306,41.164038],[-8.642763,41.164947],[-8.642709,41.164956],[-8.641737,41.164983]]";
//        List<List<TrajEntry>> queries=new ArrayList<>();
//        String[] s=lines.split("\n");
//        for (int j = 0; j < s.length; j++) {
//            String line=s[j];
//            String[] temp = line.split("\t");
//            String trajId = temp[0];
//            String trajContent = temp[1];
//
//            trajContent = trajContent.substring(2, trajContent.length() - 2); //remove head "[[" and tail "]]"
//            String[] trajTuples = trajContent.split("],\\[");
//            List<TrajEntry> query = new ArrayList<>();
//
//            String[] latLng;
//            for (int i = 0; i < trajTuples.length; i++){
//
//                double lat = 0.;
//                double lon = 0.;
//
//                latLng = trajTuples[i].split(",");
//                lat = Double.parseDouble(latLng[1]);
//                lon = Double.parseDouble(latLng[0]);
//
//                Coordinate node = new Coordinate(lat, lon);
//
//                query.add(node);
//            }
//            for (int i = 0; i < query.size(); i++) {
//                System.out.print(query.get(i).getLat()+","+query.get(i).getLng()+",");
//            }
//            System.out.println();
//            queries.add(query);
//        };
//        System.out.println(queries.size());
        List<List<TrajEntry>> queries = Test.read();
        QueryResult topK = engine.findTopK(queries.get(0), 3);
        QueryResult result = engine.findOnPath(queries.get(1));
        System.out.println((topK.toJSON(1)));
        System.out.println(result.toJSON(1));
    }

    public void updateMeta() throws IOException {
        FileWriter fr = new FileWriter(Constants.TORCH_RES_BASE_DIR+"/Torch_Porto_test/meta");
        BufferedWriter bufferedWriter=new BufferedWriter(fr);
        bufferedWriter.write("car");
        bufferedWriter.newLine();
        bufferedWriter.write(Constants.TORCH_RES_BASE_DIR+"/raw/Porto.osm.pbf");
        bufferedWriter.close();
    }


    public void mapMatching() {
        String filePath = Constants.TORCH_RES_BASE_DIR+"/raw/porto_raw_trajectory.txt"; // 替换为实际的文件路径
        String pbfFilePath=Constants.TORCH_RES_BASE_DIR+"/raw/Porto.osm.pbf";
        Test.init(baseDir,filePath,pbfFilePath);
        Transaction.getInstance().SaveAll();
    }

    public void initEngine() {
        engine=Engine.getBuilder().baseDir(baseDir).build();
//        System.out.println(1);
    }

    public List<Trajectory<TrajEntry>> rangeQuery(SearchWindow searchWindow){
        QueryResult inRange = engine.findInRange(searchWindow);
        return inRange.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> pathQuery(Trajectory trajectory){
        QueryResult onPath = engine.findOnPath(trajectory);
        return onPath.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> pathQuery(String pathName){
        QueryResult onPath = engine.findOnPath(pathName);
        return onPath.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> strictPathQuery(Trajectory trajectory){
        QueryResult onPath = engine.findOnStrictPath(trajectory);
        return onPath.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> strictPathQuery(String pathName){
        QueryResult onPath = engine.findOnStrictPath(pathName);
        return onPath.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> topkQuery(Trajectory trajectory,int k,String similarityFunction)  {
        engine=Engine.getBuilder().preferedSimilarityMeasure(similarityFunction).baseDir(baseDir).build();
        QueryResult onPath = engine.findTopK(trajectory,k);
        return onPath.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> topkQuery(Trajectory trajectory,int k){
        QueryResult onPath = engine.findTopK(trajectory,k);
        return onPath.resolvedRet;
    }


    //将traj数据插入tmdb中，原始的轨迹数据
    public void insert(String srcPath){
        BufferedReader reader = null;
        String sql="CREATE CLASS traj (traj_id int,user_id char,traj_name char,traj char);";
        Create create=new CreateImpl();
        try {
            create.create(CCJSqlParserUtil.parse(sql));
        }catch (TMDBException e){
            System.out.println(e.getMessage());
        } catch (JSQLParserException e) {
            logger.warn(e.getMessage());
        }
        try {
            // 读取文件路径
            String filePath = srcPath;
            reader = new BufferedReader(new FileReader(filePath));
            String line;
            List<List<TrajEntry>> list=new ArrayList<>();
            // 逐行读取文件内容
            while ((line = reader.readLine()) != null) {
                String[] sa=line.split("\\s+");
                String traj=sa[1];
                traj=traj.replace("[","").replace("]","").replace(",","|");
                InsertImpl insert=new InsertImpl();
                sql="Insert into traj values ("+sa[0]+",-1,"+getFileNameWithoutExtension(srcPath)+","+traj+")";
                net.sf.jsqlparser.statement.insert.Insert parse = (net.sf.jsqlparser.statement.insert.Insert)CCJSqlParserUtil.parse(sql);
                insert.insert(parse);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TMDBException e) {
            // logger.warn(e.getMessage());
            e.printError();
        } catch (JSQLParserException e) {
            logger.warn(e.getMessage());
        } finally {
            try {
                // 关闭文件读取器
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void toTMDB(String baseDir) throws JSQLParserException, TMDBException, IOException {
        String sql="select * from engine where base_dir="+baseDir;
        Statement parse = CCJSqlParserUtil.parse(sql);
        Select select=new SelectImpl();
        SelectResult selectResult = select.select(parse);
        if(selectResult.getTpl().tuplelist.isEmpty()){
            buildTMDBData(baseDir);
        }
    }

    private void buildTMDBData(String baseDir) throws JSQLParserException, TMDBException, IOException {
        String sql="inset into engine values("+baseDir+");";
        Insert insert=new InsertImpl();
        insert.insert(CCJSqlParserUtil.parse(sql));
        idVertex(baseDir);
        idEdge(baseDir);
        idEdgeRaw(baseDir);
        trajectoryTimePartial(baseDir);
    }

    private void trajectoryTimePartial(String baseDir) {
        String filePath = getFilePath(baseDir) + "trajectory_time_partial";
        List<TrajectoryTimePartial> tlist = new ArrayList<TrajectoryTimePartial>();
        try(BufferedReader reader=new BufferedReader(new FileReader(filePath))) {
            String line;
            List<List<TrajEntry>> list=new ArrayList<>();
            String[] tokens;
            String id;
            String[] span;
            String start;
            String end;
            // 逐行读取文件内容
            while ((line = reader.readLine()) != null) {
                tokens = line.split(Torch.SEPARATOR_2);
                id= tokens[0];
                span = tokens[1].split(" \\| ");
                start = span[0];
                end = span[1];
                tlist.add(new TrajectoryTimePartial(id,start,end));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void idEdgeRaw(String baseDir) {
        String filePath = getFilePath(baseDir) + "id_edge_raw";
        List<IdEdgeRaw> tlist = new ArrayList<IdEdgeRaw>();
        try(BufferedReader reader=new BufferedReader(new FileReader(filePath))) {
            String line;
            List<List<TrajEntry>> list=new ArrayList<>();
            String[] tokens;
            int id;
            String lats;
            String lngs;
            // 逐行读取文件内容
            while ((line = reader.readLine()) != null) {
                tokens = line.split(";");
                id = Integer.parseInt(tokens[0]);
                lats = tokens[1];
                lngs = tokens[2];
                tlist.add(new IdEdgeRaw(id,lats,lngs));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void idEdge(String baseDir) {
        String filePath = getFilePath(baseDir) + "id_edge";
        List<IdEdge> tlist = new ArrayList<IdEdge>();
        try(BufferedReader reader=new BufferedReader(new FileReader(filePath))) {
            String line;
            List<List<TrajEntry>> list=new ArrayList<>();
            String[] tokens;
            Integer edgeId;
            Integer vertexId1;
            Integer vertexId2;
            // 逐行读取文件内容
            while ((line = reader.readLine()) != null) {
                tokens = line.split(";");
                edgeId = Integer.parseInt(tokens[0]);
                vertexId1 = Integer.parseInt(tokens[1]);
                vertexId2 = Integer.parseInt(tokens[2]);
                tlist.add(new IdEdge(edgeId,vertexId1,vertexId2));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void idVertex(String baseDir){
        String filePath = getFilePath(baseDir) + "id_vertex";
        List<IdVertex> tlist = new ArrayList<IdVertex>();
        try(BufferedReader reader=new BufferedReader(new FileReader(filePath))) {
            String line;
            List<List<TrajEntry>> list=new ArrayList<>();
            String[] tokens;
            int id;
            Double lat;
            Double lng;
            // 逐行读取文件内容
            while ((line = reader.readLine()) != null) {
                tokens = line.split(";");
                id = Integer.parseInt(tokens[0]);
                lat = Double.parseDouble(tokens[1]);
                lng = Double.parseDouble(tokens[2]);
                tlist.add(new IdVertex(id,lat,lng));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getFilePath(String baseDir){
        return Constants.TORCH_RES_BASE_DIR+baseDir+"/Torch/";
    }
}


