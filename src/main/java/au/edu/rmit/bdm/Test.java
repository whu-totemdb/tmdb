package au.edu.rmit.bdm;

import au.edu.rmit.bdm.Torch.base.FileSetting;
import au.edu.rmit.bdm.Torch.base.PathQueryIndex;
import au.edu.rmit.bdm.Torch.base.Torch;
import au.edu.rmit.bdm.Torch.base.db.DBManager;
import au.edu.rmit.bdm.Torch.base.helper.MemoryUsage;
import au.edu.rmit.bdm.Torch.base.invertedIndex.EdgeInvertedIndex;
import au.edu.rmit.bdm.Torch.base.invertedIndex.VertexInvertedIndex;
import au.edu.rmit.bdm.Torch.base.model.*;
import au.edu.rmit.bdm.Torch.mapMatching.MapMatching;
import au.edu.rmit.bdm.Torch.mapMatching.model.TowerVertex;
import au.edu.rmit.bdm.Torch.queryEngine.Engine;
import au.edu.rmit.bdm.Torch.queryEngine.query.QueryResult;
import com.alibaba.fastjson2.function.impl.StringToAny;
import com.graphhopper.GraphHopper;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.util.CarFlagEncoder;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FlagEncoder;

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

public class Test {

    static Logger logger = LoggerFactory.getLogger(Test.class);
    static FileSetting setting = new FileSetting("Torch_Porto");


    
    public static void main(String[] args) throws IOException {
//        MapMatching mm = MapMatching.getBuilder().setBaseDir("Torch_Porto_test").build("Resources/porto_raw_trajectory.txt","Resources/porto.osm.pbf");
//        mm.start();
//
//        Engine engine = Engine.getBuilder().baseDir("Torch_Porto").build();
//        Engine engine = Engine.getBuilder().baseDir("Torch").build();
//        Engine.Builder torchPortoTest = Engine.getBuilder().baseDir("Torch_Porto_test");
//        init("Torch_Porto_test","Resources/porto_raw_trajectory.txt","Resources/porto.osm.pbf");
        Engine engine = Engine.getBuilder().baseDir("Torch_Porto_test").build();
        List<List<TrajEntry>> queries = read();
        QueryResult topK = engine.findTopK(queries.get(0), 3);
        QueryResult result = engine.findOnPath(queries.get(0));
        QueryResult inRange = engine.findInRange(-8.639847,41.159826, 50);
        System.out.println((inRange.toJSON(1)));
        System.out.println(topK.toJSON(1));
//        useOwnDataset();

    }

    public static void init(String baseDir, String trajSrcPath, String osmPath) {
        MapMatching mm = MapMatching.getBuilder().setBaseDir(baseDir).build(trajSrcPath,osmPath);
        mm.start();
        setting=new FileSetting(baseDir);

        getAfew();
        addTime();
        DBManager.init(setting);
        DBManager dbManager=DBManager.getDB();
        dbManager.run();
    }


    public static void useOwnDataset() throws IOException {
        MapMatching mm = MapMatching.getBuilder().setBaseDir("Torch_Porto_test").build("Resources/porto_raw_trajectory.txt","Resources/porto.osm.pbf");
        mm.start();
        addTime();
        Engine engine = Engine.getBuilder().baseDir("Torch_Porto_test").build();
        List<List<TrajEntry>> queries = read();
        QueryResult result = engine.findOnPath(queries.get(0));
    }

    public static List<List<TrajEntry>> read() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("data/res/raw/query.txt"));
        List<List<TrajEntry>> list = new ArrayList<>(3);

        String line;
        while((line = reader.readLine())!=null){

            String[] temp = line.split("\t");
            String trajId = temp[0];
            String trajContent = temp[1];

            trajContent = trajContent.substring(2, trajContent.length() - 2); //remove head "[[" and tail "]]"
            String[] trajTuples = trajContent.split("],\\[");
            List<TrajEntry> query = new ArrayList<>();

            String[] latLng;
            for (int i = 0; i < trajTuples.length; i++){

                double lat = 0.;
                double lon = 0.;

                    latLng = trajTuples[i].split(",");
                    lat = Double.parseDouble(latLng[1]);
                    lon = Double.parseDouble(latLng[0]);

                Coordinate node = new Coordinate(lat, lon);

                query.add(node);
            }
            list.add(query);
        }
        return list;
    }

    private static void addTime()  {

        // load trajectory edge representation
//        BufferedReader reader = new BufferedReader(new FileReader(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH_PARTIAL));
//        BufferedWriter writer = new BufferedWriter(new FileWriter(setting.TRAJECTORY_START_END_TIME_PARTIAL));
//        String line;
//        Map<String, Integer> map = new LinkedHashMap<>(); //trajectory id - number of edges
//        while((line = reader.readLine())!= null){
//            String[] tokens = line.split("\t");
//            map.put(tokens[0], tokens[1].split(",").length);
//        }

        //从trajectory edge partial中读取出数据
        String edgePartial = getFileNameWithoutExtension(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH_PARTIAL);
        PlainSelect edgePartialSelect = new PlainSelect().addSelectItems(new AllColumns());
        edgePartialSelect.withFromItem(new Table(edgePartial));
        Expression whereExpression =new EqualsTo(new Column().withColumnName(edgePartial),new StringValue(edgePartial));
        edgePartialSelect.setWhere(whereExpression);
        SelectResult edge = Transaction.getInstance().query(new Select().withSelectBody(edgePartialSelect));

        List<Tuple> tuplelist = edge.getTpl().tuplelist;
        Map<String, Integer> map = tuplelist.stream().collect(Collectors.toMap(e -> (String) e.tuple[0], e -> ((String) e.tuple[0]).split(",").length));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long cur = System.currentTimeMillis();
        long begin = cur - 60 * 60 *  24 * 164 * 1000L; //150 days ago
        long end = cur - 60 * 60 *  24 * 154 * 1000L; //140 days age
        long max = end - begin;

        System.out.println("begin date: "+sdf.format(begin));
        System.out.println("end date: "+sdf.format(end));


        List<String> l = new ArrayList<>(200000);
        String separator = Torch.TIME_SEP;
        Random initial_span = new Random(17);
        Random span = new Random(21);
        trajectoryTimePartialCreate();
        List<Expression> expressions = new ArrayList<>();
        int counter = 0;
        for (Map.Entry<String, Integer> entry : map.entrySet()){
            if (++counter == 30){
                initial_span.setSeed(counter);
                span.setSeed(100000 - counter);
            }

            long individual_start;
            long individual_end;

            while(true) {
                long temp = initial_span.nextLong();
                if (temp <0L) continue;
                individual_start = begin + temp % max;
                individual_end = individual_start + entry.getValue() * (span.nextInt(60 * 1000) + 100000);
                if (individual_end < end) break;
            }

            Date d1 = new Date(individual_start);
            Date d2 = new Date(individual_end);

            ExpressionList expressionList = new ExpressionList()
                    .addExpressions(new StringValue(setting.TorchBase))
                    .addExpressions(new LongValue(""+entry.getKey()))
                    .addExpressions(new StringValue(sdf.format(d1)))
                    .addExpressions(new StringValue(sdf.format(d2)));
//            String ret = entry.getKey() + Torch.SEPARATOR_2 + sdf.format(d1)+separator+sdf.format(d2);
//            writer.write(ret);
//            writer.newLine();
            RowConstructor rowConstructor = new RowConstructor().withExprList(expressionList);
            expressions.add(rowConstructor);
        }
        ValuesStatement valuesStatement = new ValuesStatement().withExpressions(new ExpressionList().withExpressions(expressions));
        trajectoryTimePartialInsert(valuesStatement);
//        writer.flush();
//        writer.close();
        //range
    }

    private static void trajectoryTimePartialCreate()  {
        //创建id_edge 表
        String trajectoryTime = getFileNameWithoutExtension(setting.TRAJECTORY_START_END_TIME_PARTIAL);

        //使用jsqlparser构建一个create table statement，丢入transaction中进行创建
        ColumnDefinition traj_name = new ColumnDefinition("traj_name", new ColDataType("char"));
        ColumnDefinition id = new ColumnDefinition("id", new ColDataType("int"));
        ColumnDefinition date1 = new ColumnDefinition("date1", new ColDataType("char"));
        ColumnDefinition date2 = new ColumnDefinition("date2", new ColDataType("char"));
        // Initialize the ArrayList and add the items
        List<ColumnDefinition> timePartialColumns = new ArrayList<>();
        timePartialColumns.add(traj_name);
        timePartialColumns.add(id);
        timePartialColumns.add(date1);
        timePartialColumns.add(date2);
        CreateTable createTrajectoryEdgeTable = new CreateTable()
                .withTable(new Table(trajectoryTime))
                .addColumnDefinitions(timePartialColumns);
        Transaction.getInstance().query(createTrajectoryEdgeTable);
        Transaction.getInstance().SaveAll();
    }

    private static void trajectoryTimePartialInsert(ValuesStatement valuesStatement)  {
        String trajectoryTime = getFileNameWithoutExtension(setting.TRAJECTORY_START_END_TIME_PARTIAL);

        //insert value into trajectory_time_partial
        Column column_traj_name = new Column("traj_name");
        Column column_id = new Column("id");
        Column date1 = new Column("date1");
        Column date2 = new Column("date2");

        // 将上述的column加入将要insert的column中
        List<Column> insertColumns = new ArrayList<>();
        insertColumns.add(column_traj_name);
        insertColumns.add(column_id);
        insertColumns.add(date1);
        insertColumns.add(date2);

        Insert insert = new Insert().withTable(new Table(trajectoryTime)).withColumns(insertColumns).withSelect(new Select().withSelectBody(valuesStatement));
        Transaction.getInstance().query(insert);
    }


    private static void buildStreetNameLookupDBfromFile() throws IOException {
        Map<String, String> lookup = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(setting.ID_EDGE_RAW));
        String line;

        while((line = reader.readLine())!=null){
            String[] tokens = line.split(";", -1);
            int lastIdx = tokens.length - 1;
            String name = tokens[lastIdx];
            String id = tokens[0];

            if (name.length() == 0) continue;
            lookup.merge(name, id, (a, b) -> a + "," + b);
        }

//        DBManager2 db= DBManager2.getDB();
        DBManager db= new DBManager(setting);
        db.buildTable(setting.EDGENAME_ID_TABLE, true);
        for (Map.Entry<String, String> entry: lookup.entrySet())
            db.insert(setting.EDGENAME_ID_TABLE, entry.getKey(), entry.getValue());

        db.closeConn();
    }

    private static void streetNameLookup() throws IOException {
        Map<String, String> nameIdLookup = new HashMap<>();

        BufferedReader reader = new BufferedReader(new FileReader(setting.ID_EDGE_RAW));
        String line;
        while((line = reader.readLine())!=null){
            String[] tokens = line.split(";", -1);
            String id = tokens[0];
            String name = tokens[tokens.length - 1];
            if (name.length()!=0)
                nameIdLookup.put(name, id);
            if (name.equals("Largo 5 de Outubro"))
                System.out.println(id);
        }


        System.out.println(nameIdLookup.get("Largo 5 de Outubro"));

    }

    private static void toDB(){
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("cannot load mysql driver");
            System.exit(1);
        }

    }

    private static void addLenToEdgeLookuptable() throws IOException{
        BufferedReader edgeReader = new BufferedReader(new FileReader(setting.ID_EDGE_LOOKUP));
        BufferedReader rawReader = new BufferedReader(new FileReader(setting.ID_EDGE_RAW));

        List<String> edges = new ArrayList<>(10000);
        String line;
        String raw[];
        double dist;
        while((line = edgeReader.readLine())!=null){
            raw = rawReader.readLine().split(";");
            dist = Double.parseDouble(raw[raw.length - 3]);
            edges.add(line + ";" + dist);
        }

        edgeReader.close();
        rawReader.close();

        BufferedWriter writer = new BufferedWriter(new FileWriter(setting.ID_EDGE_LOOKUP));
        for (String edge : edges){

            writer.write(edge);
            writer.newLine();
        }

        writer.flush();
        writer.close();
    }

    //todo 这里改vertex和edge的partial
    private static void getAfew() {
//        BufferedReader edgeReader = new BufferedReader(new FileReader(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH));
//        BufferedReader vertexReader = new BufferedReader(new FileReader(setting.TRAJECTORY_VERTEX_REPRESENTATION_PATH));

        //需要创建名为trajectory_edge_partial的代理类
        String trajectory_edge_partial = getFileNameWithoutExtension(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH_PARTIAL);
        CreateDeputyClass createEdgePartial= new CreateDeputyClass();
        //创建代理类的代理类名
        createEdgePartial.setDeputyClass(new Table(trajectory_edge_partial));

        //设置select
        String edge = getFileNameWithoutExtension(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH);
        PlainSelect edgeSelect = new PlainSelect();
        edgeSelect.addSelectItems(new AllColumns());
        edgeSelect.setFromItem(new Table(getFileNameWithoutExtension(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH)));
        Expression whereExpression =new EqualsTo(new Column().withColumnName(edge),new StringValue(edge));
        edgeSelect.setWhere(whereExpression);
        Limit limit = new Limit();
        limit.setRowCount(new LongValue(""+100000));
        edgeSelect.setLimit(limit);

        //将select添加到代理类中
        createEdgePartial.setSelect(new Select().withSelectBody(edgeSelect));

        //执行query
        Transaction.getInstance().query(createEdgePartial);

        //需要创建名为trajectory_vertex_partial的代理类
        String trajectory_vertex_partial = getFileNameWithoutExtension(setting.TRAJECTORY_VERTEX_REPRESENTATION_PATH_PARTIAL);
        CreateDeputyClass createVertexPartial= new CreateDeputyClass();
        //创建代理类的代理类名
        createVertexPartial.setDeputyClass(new Table(trajectory_vertex_partial));

        //设置select
        String vertex = getFileNameWithoutExtension(setting.TRAJECTORY_VERTEX_REPRESENTATION_PATH);
        PlainSelect vertexSelect = new PlainSelect();
        vertexSelect.addSelectItems(new AllColumns());
        vertexSelect.setFromItem(new Table(getFileNameWithoutExtension(setting.TRAJECTORY_VERTEX_REPRESENTATION_PATH)));
        whereExpression =new EqualsTo(new Column().withColumnName(vertex),new StringValue(vertex));
        vertexSelect.setWhere(whereExpression);
        vertexSelect.setLimit(limit);

        //将select添加到代理类中
        createVertexPartial.setSelect(new Select().withSelectBody(vertexSelect));

        //执行query
        Transaction.getInstance().query(createVertexPartial);

//        List<String> edgeList = new ArrayList<>(200001);
//        List<String> vertexList = new ArrayList<>(200001);
//        String line1, line2;
//        int i = 0;
//        while((line1 = edgeReader.readLine()) != null){
//            edgeList.add(line1);
//            line2 = vertexReader.readLine();
//            vertexList.add(line2);
//            if (++i % 100000 == 0){
//                break;
//            }
//        }
////        BufferedWriter writer = new BufferedWriter(new FileWriter(setting.TRAJECTORY_VERTEX_REPRESENTATION_PATH+"_partial.txt"));
//        BufferedWriter writer = new BufferedWriter(new FileWriter(setting.TRAJECTORY_VERTEX_REPRESENTATION_PATH_PARTIAL));
//        for (String line : vertexList){
//            writer.write(line);
//            writer.newLine();
//        }
//        writer.flush();
//        writer.close();
//
////        writer = new BufferedWriter(new FileWriter(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH+"_partial.txt"));
//        writer = new BufferedWriter(new FileWriter(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH_PARTIAL));
//        for (String line:edgeList){
//            writer.write(line);
//            writer.newLine();
//        }
//        writer.flush();
//        writer.close();
    }

    private static void genEdgeInvertedIndex() throws IOException {

        BufferedReader bufferedReader = new BufferedReader(new FileReader(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH_PARTIAL));
        EdgeInvertedIndex edgeInvertedIndex = new EdgeInvertedIndex(setting);

        String line;
        String[] tokens;
        String[] edges;

        MemoryUsage.start();

        int i = 0;
        while((line = bufferedReader.readLine()) != null){

            if (++i % 10000 == 0){
                System.err.println("current progress: "+i);
                MemoryUsage.printCurrentMemUsage("");
                if (i == 100000) break;
            }
            tokens = line.split("\t");
            edges = tokens[1].split(",");

            Trajectory<TrajEntry> t = new Trajectory<>();
            t.id = tokens[0];

            for (String edge : edges)
                t.edges.add(new TorEdge(Integer.parseInt(edge), null, null, 0));


            edgeInvertedIndex.index(t);
        }
        MemoryUsage.printCurrentMemUsage("");

        edgeInvertedIndex.saveCompressed(setting.EDGE_INVERTED_INDEX);
    }

    private static void genVertexInvertedIndex() throws IOException {

        BufferedReader bufferedReader = new BufferedReader(new FileReader(setting.TRAJECTORY_VERTEX_REPRESENTATION_PATH_PARTIAL));
        VertexInvertedIndex vertexInvertedIndex= new VertexInvertedIndex(setting);

        String line;
        String[] tokens;
        String[] vertices;

        MemoryUsage.start();

        int i = 0;
        while((line = bufferedReader.readLine()) != null){

            if (++i % 10000 == 0){
                System.err.println("current progress: "+i);
                MemoryUsage.printCurrentMemUsage("");
                if (i == 100000) break;
            }
            tokens = line.split("\t");
            vertices = tokens[1].split(",");

            Trajectory<TrajEntry> t = new Trajectory<>();
            t.id = tokens[0];

            for (String vertex : vertices)
               t.add(new TowerVertex(0,0, Integer.valueOf(vertex)));


            vertexInvertedIndex.index(t);
        }
        MemoryUsage.printCurrentMemUsage("");

        vertexInvertedIndex.saveCompressed(setting.VERTEX_INVERTED_INDEX);
    }

    private static void initGH(){
        GraphHopper hopper = new GraphHopperOSM();
        hopper.setDataReaderFile("Resources/Porto.osm.pbf");
        hopper.setGraphHopperLocation(setting.hopperURI);
        FlagEncoder vehicle = new CarFlagEncoder();
        hopper.setEncodingManager(new EncodingManager(vehicle));
        hopper.getCHFactoryDecorator().setEnabled(false);
        hopper.importOrLoad();
    }
}
