package au.edu.rmit.bdm.Torch.mapMatching;

import au.edu.rmit.bdm.Torch.base.FileSetting;
import au.edu.rmit.bdm.Torch.base.Torch;
import au.edu.rmit.bdm.Torch.base.invertedIndex.EdgeInvertedIndex;
import au.edu.rmit.bdm.Torch.base.invertedIndex.InvertedIndex;
import au.edu.rmit.bdm.Torch.base.invertedIndex.VertexInvertedIndex;
import au.edu.rmit.bdm.Torch.base.model.*;
import au.edu.rmit.bdm.Torch.base.model.TorEdge;
import au.edu.rmit.bdm.Torch.mapMatching.algorithm.TorGraph;
import au.edu.rmit.bdm.Torch.mapMatching.model.TowerVertex;
import com.github.davidmoten.geo.GeoHash;
import com.graphhopper.storage.Graph;
import com.graphhopper.storage.NodeAccess;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.util.FileOperation;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static au.edu.rmit.bdm.Torch.base.helper.FileUtil.*;
import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

/**
 * The class is for saving relevant information to disk.
 *
 * These includes:
 *      ~ vertexId -- GPS Coordinate table
 *
 *      ~ edgeId -- edgeInfo table
 *      ~ edgeId -- vertexId table
 *
 *      ~ map-matched trajectory represented by vertices
 *      ~ map-matched trajectory represented by edges
 *
 *      ~ edge inverted invertedIndex( trajectory ids)
 *      ~ vertex inverted invertedIndex( trajectory ids)
 */
public class TorSaver {

    private static Logger logger = LoggerFactory.getLogger(TorSaver.class);
    private TorGraph graph;
    private boolean append = false;
    private FileSetting setting;
    public InvertedIndex edgeInvertedList;
    public InvertedIndex vertexInvertedIndex;

    private Transaction transaction;


    public TorSaver(TorGraph graph, FileSetting setting)  {
        this.setting = setting;
        this.graph = graph;
        edgeInvertedList = new EdgeInvertedIndex(setting);
        vertexInvertedIndex = new VertexInvertedIndex(setting);
        this.transaction=Transaction.getInstance();
    }

    /**
     * Once a batch of trajectories have been mapped, we saveUncompressed it using a separate thread.
     *
     * @param mappedTrajectories trajectories to be saved
     * @param saveAll false -- asyncSave trajectory data only
     *                true -- asyncSave everything( It should only be set to true if it is the last batch.)
     *                As the method is expected to be called multiple times to write different batches of trajectories,
     *                other information should only be saved once.
     *
     *
     */
    public synchronized void Save(final List<Trajectory<TowerVertex>> mappedTrajectories,final List<Trajectory<TrajEntry>> rawTrajectories, final boolean saveAll) {

        if (!graph.isBuilt)
            throw new IllegalStateException("should be called after TorGraph initialization");

        ExecutorService thread = Executors.newSingleThreadExecutor();
        thread.execute(() -> {
            _save(mappedTrajectories,rawTrajectories, saveAll);
        });
        thread.shutdown();
        graph.isSaved=true;
        try {
            thread.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("{}", e);
        }
    }

    private void _save(final List<Trajectory<TowerVertex>> mappedTrajectories,final List<Trajectory<TrajEntry>> rawTrajectories, final boolean saveAll)  {

        saveMappedTrajectories(mappedTrajectories);  // for purpose of debugging
        //trajectoryMap.saveAll(rawTrajectories);

        if (saveAll) {
            saveMeta();
            saveIdVertexLookupTable();
            saveEdges();
            getAfew();
            addTime();
            edgeInvertedList.saveCompressed(setting.EDGE_INVERTED_INDEX);
            vertexInvertedIndex.saveCompressed(setting.VERTEX_INVERTED_INDEX);


            //trajectoryMap.cleanUp();
        }
    }

    private void saveMeta() {
        ensureExistence(setting.metaURI);
        try(FileWriter fw = new FileWriter(setting.metaURI);
            BufferedWriter writer = new BufferedWriter(fw))
        {
            writer.write(graph.vehicleType);
            writer.newLine();
            writer.write(graph.OSMPath);
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveIdVertexLookupTable()  {


        Graph hopperGraph = graph.getGH().getGraphHopperStorage().getBaseGraph();
        int numNodes = hopperGraph.getNodes();

        NodeAccess nodeAccess = hopperGraph.getNodeAccess();
//        ensureExistence(setting.ID_VERTEX_LOOKUP)
        idVertexCreate();
        List<Expression> expressions = new ArrayList<>();
        for (int i = 0; i < numNodes; i++){
            ExpressionList expressionList = new ExpressionList()
                    .addExpressions(new StringValue(setting.TorchBase))
                    .addExpressions(new LongValue(""+i))
                    .addExpressions(new DoubleValue("" + nodeAccess.getLatitude(i)))
                    .addExpressions(new DoubleValue("" + nodeAccess.getLongitude(i)));
            RowConstructor rowConstructor = new RowConstructor().withExprList(expressionList);
            expressions.add(rowConstructor);
        }
        ValuesStatement valuesStatement = new ValuesStatement().withExpressions(new ExpressionList().withExpressions(expressions));
        idVertexInsert(valuesStatement);
//        try(BufferedWriter writer = new BufferedWriter(new FileWriter(setting.ID_VERTEX_LOOKUP, false))){
//            StringBuilder builder = new StringBuilder();
//            for (int i = 0; i < numNodes; i++){
//                builder.append(i).append(";")
//                        .append(nodeAccess.getLatitude(i)).append(";")
//                        .append(nodeAccess.getLongitude(i));
//
//                writer.write(builder.toString());
//                writer.newLine();
//                builder.setLength(0);
//            }
//            writer.flush();
//        }catch (IOException e){
//            logger.error(e.getMessage());
//        }
    }

    private void saveEdges()  {
        Collection<TorEdge> allEdges = graph.allEdges.values();

        List<TorEdge> edges = new ArrayList<>(allEdges);
        edges.sort(Comparator.comparing(e -> e.id));

//        ensureExistence(setting.ID_EDGE_RAW);
        idEdgeRawCreate();
        idEdgeCreate();

        List<Expression> raw_expressions = new ArrayList<>();
        List<Expression> expressions = new ArrayList<>();

        Set<Integer> visited = new HashSet<>();
        for (TorEdge edge : edges){
            if (visited.contains(edge.id)) continue;
            visited.add(edge.id);
            String[] split = edge.convertToDatabaseForm().split(Torch.SEPARATOR_1);
            ExpressionList rawExpressionList = new ExpressionList()
                    .addExpressions(new StringValue(setting.TorchBase))
                    .addExpressions(new LongValue(split[0]))
                    .addExpressions(new StringValue(split[1]))
                    .addExpressions(new StringValue(split[2]))
                    .addExpressions(new DoubleValue(split[3]))
                    .addExpressions(new StringValue(split[4]))
                    .addExpressions(new StringValue(split[5]));
            RowConstructor rowRawConstructor = new RowConstructor().withExprList(rawExpressionList);
            raw_expressions.add(rowRawConstructor);

            ExpressionList expressionList = new ExpressionList()
                    .addExpressions(new StringValue(setting.TorchBase))
                    .addExpressions(new LongValue(""+edge.id))
                    .addExpressions(new LongValue(""+graph.vertexIdLookup.get(edge.baseVertex.hash)))
                    .addExpressions(new LongValue(""+graph.vertexIdLookup.get(edge.adjVertex.hash)))
                    .addExpressions(new DoubleValue("" + edge.getLength()));
            RowConstructor rowConstructor = new RowConstructor().withExprList(expressionList);
            expressions.add(rowConstructor);
        }
        ValuesStatement rawValuesStatement = new ValuesStatement().withExpressions(new ExpressionList().withExpressions(raw_expressions));
        ValuesStatement valuesStatement = new ValuesStatement().withExpressions(new ExpressionList().withExpressions(expressions));
        idEdgeRawInsert(rawValuesStatement);
        idEdgeInsert(valuesStatement);

//        try(BufferedWriter rawWriter = new BufferedWriter(new FileWriter(setting.ID_EDGE_RAW));
//            BufferedWriter writer = new BufferedWriter(new FileWriter(setting.ID_EDGE_LOOKUP))) {
//
//            StringBuilder builder = new StringBuilder();
//            Set<Integer> visited = new HashSet<>();
//
//            for (TorEdge edge : edges){
//
//                if (visited.contains(edge.id)) continue;
//                visited.add(edge.id);
//
//                rawWriter.write(edge.convertToDatabaseForm());
//                rawWriter.newLine();
//
//                builder.append(edge.id).append(Torch.SEPARATOR_1)
//                       .append(graph.vertexIdLookup.get(edge.baseVertex.hash)).append(Torch.SEPARATOR_1)
//                       .append(graph.vertexIdLookup.get(edge.adjVertex.hash)).append(Torch.SEPARATOR_1)
//                       .append(edge.getLength());
//
//                writer.write(builder.toString());
//                writer.newLine();
//                builder.setLength(0);
//            }
//
//            rawWriter.flush();
//            writer.flush();
//
//        }catch (IOException e){
//            logger.error(e.getMessage());
//        }
    }

    private void saveMappedTrajectories(List<Trajectory<TowerVertex>> mappedTrajectories)  {

//        if (!append) ensureExistence(setting.TRAJECTORY_VERTEX_REPRESENTATION_PATH);

        //invertedIndex trajectories
        vertexInvertedIndex.indexAll(mappedTrajectories);
        edgeInvertedList.indexAll(mappedTrajectories);

        trajectoryVertexCreate();
        trajectoryEdgeCreate();
        List<Expression> vertexExpressions = new ArrayList<>();
        List<Expression> edgeExpressions = new ArrayList<>();
        for (Trajectory<TowerVertex> traj : mappedTrajectories) {
            ExpressionList vertexExpressionList = new ExpressionList()
                    .addExpressions(new StringValue(setting.TorchBase))
                    .addExpressions(new LongValue(traj.id));
            StringBuilder vertexBuilder = new StringBuilder();
            String hash;
            for (TowerVertex vertex : traj) {
                hash = GeoHash.encodeHash(vertex.lat, vertex.lng);
                Integer id = graph.vertexIdLookup.get(hash);

                if (id == null) {
                    logger.error("a mapped edge is missing when processing trajectory id "+ traj.id);
                }
                else {
                    vertexBuilder.append(id).append(",");
                }
            }
            vertexExpressionList.addExpressions(new StringValue(vertexBuilder.toString()));
            RowConstructor vertexConstructor = new RowConstructor().withExprList(vertexExpressionList);
            vertexExpressions.add(vertexConstructor);

            ExpressionList edgeExpressionList = new ExpressionList()
                    .addExpressions(new StringValue(setting.TorchBase))
                    .addExpressions(new LongValue(traj.id));
            StringBuilder edgeBuilder = new StringBuilder();
            Iterator<TorEdge> iterator = traj.edges.iterator();
            while(iterator.hasNext()) {
                TorEdge curEdge = iterator.next();
                edgeBuilder.append(curEdge.id).append(",");
            }
            edgeExpressionList.addExpressions(new StringValue(edgeBuilder.toString()));
            RowConstructor edgeConstructor = new RowConstructor().withExprList(edgeExpressionList);
            edgeExpressions.add(edgeConstructor);
        }
        ValuesStatement vertexValuesStatement = new ValuesStatement().withExpressions(new ExpressionList().withExpressions(vertexExpressions));
        ValuesStatement edgevaluesStatement = new ValuesStatement().withExpressions(new ExpressionList().withExpressions(edgeExpressions));
        trajectoryVertexInsert(vertexValuesStatement);
        trajectoryEdgeInsert(edgevaluesStatement);

//        //write vertex id representation of trajectories.
//        try(BufferedWriter writer = new BufferedWriter(new FileWriter(setting.TRAJECTORY_VERTEX_REPRESENTATION_PATH,append))) {
//
//            StringBuilder trajBuilder = new StringBuilder();
//            String hash;
////            for (Trajectory<TowerVertex> traj : mappedTrajectories) {
////                trajBuilder.append(traj.id).append(";");
////
////                for (TowerVertex vertex : traj) {
////                    hash = GeoHash.encodeHash(vertex.lat, vertex.lng);
////                    Integer id = graph.vertexIdLookup.get(hash);
////
////                    if (id == null)
////                        logger.error("a mapped edge is missing when processing trajectory id "+ traj.id);
////                    else
////                        trajBuilder.append(id).append(";");
////                }
////
////                //remove the tail ";" character
////                trajBuilder.setLength(trajBuilder.length()-1);
////                writer.write(trajBuilder.toString());
////                writer.newLine();
////
////                trajBuilder.setLength(0);
////            }
//            for (Trajectory<TowerVertex> traj : mappedTrajectories) {
//                trajBuilder.append(traj.id).append("\t");
//
//                for (TowerVertex vertex : traj) {
//                    hash = GeoHash.encodeHash(vertex.lat, vertex.lng);
//                    Integer id = graph.vertexIdLookup.get(hash);
//
//                    if (id == null) {
//                        logger.error("a mapped edge is missing when processing trajectory id "+ traj.id);
//                    }
//                    else {
//                        trajBuilder.append(id).append(",");
//                    }
//                }
//
//                //remove the tail ";" character
//                trajBuilder.setLength(trajBuilder.length()-1);
//                writer.write(trajBuilder.toString());
//                writer.newLine();
//
//                trajBuilder.setLength(0);
//            }
//
//            writer.flush();
//
//        }catch (IOException e){
//            e.printStackTrace();
//        }
//
//        //write edge id representation of trajectories.
//        try(BufferedWriter writer = new BufferedWriter(new FileWriter(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH, append))) {
//
//            StringBuilder trajBuilder = new StringBuilder();
//            Iterator<TorEdge> iterator;
//            TorEdge curEdge;
//
////            for (Trajectory<TowerVertex> traj : mappedTrajectories) {
////
////                trajBuilder.append(traj.id).append(";");
////                iterator = traj.edges.iterator();
////
////                while(iterator.hasNext()) {
////                    curEdge = iterator.next();
////                    trajBuilder.append(curEdge.id).append(";");
////                }
////
////                //remove the tail ";" character
////                trajBuilder.setLength(trajBuilder.length()-1);
////                writer.write(trajBuilder.toString());
////                writer.newLine();
////
////                trajBuilder.setLength(0);
////            }
//
//            for (Trajectory<TowerVertex> traj : mappedTrajectories) {
//
//                trajBuilder.append(traj.id).append("\t");
//                iterator = traj.edges.iterator();
//
//                while(iterator.hasNext()) {
//                    curEdge = iterator.next();
//                    trajBuilder.append(curEdge.id).append(",");
//                }
//
//                //remove the tail ";" character
//                trajBuilder.setLength(trajBuilder.length()-1);
//                writer.write(trajBuilder.toString());
//                writer.newLine();
//
//                trajBuilder.setLength(0);
//            }
//
//            writer.flush();
//
//        }catch (IOException e){
//            e.printStackTrace();
//        }

        append = true;
    }

    private void addTime() {

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

    //todo 这里改vertex和edge的partial
    private void getAfew() {
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

    private void idVertexCreate() {
        String id_vertex = getFileNameWithoutExtension(setting.ID_VERTEX_LOOKUP);
        //使用jsqlparser构建一个create table statement
        //traj_name标识一条轨迹
        ColumnDefinition traj_name = new ColumnDefinition("traj_name", new ColDataType("char"));
        ColumnDefinition id = new ColumnDefinition("id", new ColDataType("int"));
        ColumnDefinition lat = new ColumnDefinition("lat", new ColDataType("double"));
        ColumnDefinition lng = new ColumnDefinition("lng", new ColDataType("double"));
        // Initialize the ArrayList and add the items
        List<ColumnDefinition> columns = new ArrayList<>();
        columns.add(traj_name);
        columns.add(id);
        columns.add(lat);
        columns.add(lng);
        CreateTable createTable = new CreateTable().withTable(new Table(id_vertex)).addColumnDefinitions(columns);
        transaction.query(createTable);
//        transaction.SaveAll();
    }
    private void idEdgeRawCreate(){
        //创建id_edge_raw 表
        String id_edge_raw = getFileNameWithoutExtension(setting.ID_EDGE_RAW);

        //使用jsqlparser构建一个create table statement,丢入transaction中进行创建
        ColumnDefinition traj_name = new ColumnDefinition("traj_name", new ColDataType("char"));
        ColumnDefinition id = new ColumnDefinition("id", new ColDataType("int"));
        ColumnDefinition lat = new ColumnDefinition("lat", new ColDataType("char"));
        ColumnDefinition lng = new ColumnDefinition("lng", new ColDataType("car"));
        ColumnDefinition length = new ColumnDefinition("length", new ColDataType("double"));
        ColumnDefinition isForward = new ColumnDefinition("is_forward", new ColDataType("bool"));
        ColumnDefinition isBackward = new ColumnDefinition("is_backward", new ColDataType("bool"));
        // Initialize the ArrayList and add the items
        List<ColumnDefinition> columns = new ArrayList<>();
        columns.add(traj_name);
        columns.add(id);
        columns.add(lat);
        columns.add(lng);
        columns.add(length);
        columns.add(isForward);
        columns.add(isBackward);
        CreateTable createTable = new CreateTable().withTable(new Table(id_edge_raw)).addColumnDefinitions(columns);
        transaction.query(createTable);
//        transaction.SaveAll();
    }
    private void idEdgeCreate() {
        //创建id_edge 表
        String id_edge = getFileNameWithoutExtension(setting.ID_EDGE_LOOKUP);

        //使用jsqlparser构建一个create table statement，丢入transaction中进行创建
        ColumnDefinition traj_name = new ColumnDefinition("traj_name", new ColDataType("char"));
        ColumnDefinition id = new ColumnDefinition("id", new ColDataType("int"));
        ColumnDefinition baseVertex = new ColumnDefinition("base_vertex", new ColDataType("int"));
        ColumnDefinition adjVertex = new ColumnDefinition("adj_vertex", new ColDataType("int"));
        ColumnDefinition edge_length = new ColumnDefinition("edge_length", new ColDataType("double"));
        // Initialize the ArrayList and add the items
        List<ColumnDefinition> idEdgeColumns = new ArrayList<>();
        idEdgeColumns.add(traj_name);
        idEdgeColumns.add(id);
        idEdgeColumns.add(baseVertex);
        idEdgeColumns.add(adjVertex);
        idEdgeColumns.add(edge_length);
        CreateTable createIdEdgeTable = new CreateTable().withTable(new Table(id_edge)).addColumnDefinitions(idEdgeColumns);
        transaction.query(createIdEdgeTable);
//        transaction.SaveAll();
    }
    private void trajectoryEdgeCreate() {
        //创建id_edge 表
        String trajectoryEdge = getFileNameWithoutExtension(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH);

        //使用jsqlparser构建一个create table statement，丢入transaction中进行创建
        ColumnDefinition traj_name = new ColumnDefinition("traj_name", new ColDataType("char"));
        ColumnDefinition id = new ColumnDefinition("id", new ColDataType("int"));
        ColumnDefinition edges = new ColumnDefinition("edges", new ColDataType("char"));
        // Initialize the ArrayList and add the items
        List<ColumnDefinition> trajectoryEdgeColumns = new ArrayList<>();
        trajectoryEdgeColumns.add(traj_name);
        trajectoryEdgeColumns.add(id);
        trajectoryEdgeColumns.add(edges);
        CreateTable createTrajectoryEdgeTable = new CreateTable()
                .withTable(new Table(trajectoryEdge))
                .addColumnDefinitions(trajectoryEdgeColumns);
        transaction.query(createTrajectoryEdgeTable);
//        transaction.SaveAll();
    }
    private void trajectoryVertexCreate() {
        //创建id_edge 表
        String trajectoryVertex = getFileNameWithoutExtension(setting.TRAJECTORY_VERTEX_REPRESENTATION_PATH);

        //使用jsqlparser构建一个create table statement，丢入transaction中进行创建
        ColumnDefinition traj_name = new ColumnDefinition("traj_name", new ColDataType("char"));
        ColumnDefinition id = new ColumnDefinition("id", new ColDataType("int"));
        ColumnDefinition vertexs = new ColumnDefinition("vertexs", new ColDataType("char"));
        // Initialize the ArrayList and add the items
        List<ColumnDefinition> trajectoryVertexColumns = new ArrayList<>();
        trajectoryVertexColumns.add(traj_name);
        trajectoryVertexColumns.add(id);
        trajectoryVertexColumns.add(vertexs);
        CreateTable createTrajectoryVertexTable = new CreateTable()
                .withTable(new Table(trajectoryVertex))
                .addColumnDefinitions(trajectoryVertexColumns);
        transaction.query(createTrajectoryVertexTable);
//        transaction.SaveAll();
    }

    private void trajectoryTimePartialCreate()  {
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
//        Transaction.getInstance().SaveAll();
    }

    private void idVertexInsert(ValuesStatement valuesStatement){
        String id_vertex = getFileNameWithoutExtension(setting.ID_VERTEX_LOOKUP);
        //insert value into id_vertex
        Column column_traj_name=new Column("traj_name");
        Column column_id = new Column("id");
        Column column_lat = new Column("lat");
        Column column_lng = new Column("lng");
        // Initialize the ArrayList and add the items
        List<Column> insertColumns = new ArrayList<>();
        insertColumns.add(column_id);
        insertColumns.add(column_lat);
        insertColumns.add(column_lng);
        Insert insert = new Insert().withTable(new Table(id_vertex)).withColumns(insertColumns).withSelect(new Select().withSelectBody(valuesStatement));
        transaction.query(insert);
    }
    private void idEdgeRawInsert(ValuesStatement valuesStatement){
        String id_edge_raw = getFileNameWithoutExtension(setting.ID_EDGE_RAW);

        //insert value into id_edge_raw
        Column column_traj_name = new Column("traj_name");
        Column column_id = new Column("id");
        Column column_lat = new Column("lat");
        Column column_lng = new Column("lng");
        Column column_length = new Column("length");
        Column column_isForward = new Column("is_forward");
        Column column_isBackward = new Column("is_backward");

        // 将上述的column加入将要insert的column中
        List<Column> insertColumns = new ArrayList<>();
        insertColumns.add(column_traj_name);
        insertColumns.add(column_id);
        insertColumns.add(column_lat);
        insertColumns.add(column_lng);
        insertColumns.add(column_length);
        insertColumns.add(column_isForward);
        insertColumns.add(column_isBackward);

        Insert insert = new Insert().withTable(new Table(id_edge_raw)).withColumns(insertColumns).withSelect(new Select().withSelectBody(valuesStatement));
        transaction.query(insert);
    }
    private void idEdgeInsert(ValuesStatement valuesStatement){
        String id_edge = getFileNameWithoutExtension(setting.ID_EDGE_LOOKUP);

        //insert value into id_edge
        Column column_traj_name = new Column("traj_name");
        Column column_id = new Column("id");
        Column column_baseVertext = new Column("base_vertex");
        Column column_adjVertex = new Column("adj_vertex");
        Column column_edgeLength = new Column("edge_length");

        // 将上述的column加入将要insert的column中
        List<Column> insertColumns = new ArrayList<>();
        insertColumns.add(column_traj_name);
        insertColumns.add(column_id);
        insertColumns.add(column_baseVertext);
        insertColumns.add(column_adjVertex);
        insertColumns.add(column_edgeLength);

        Insert insert = new Insert().withTable(new Table(id_edge)).withColumns(insertColumns).withSelect(new Select().withSelectBody(valuesStatement));
        transaction.query(insert);
    }
    private void trajectoryEdgeInsert(ValuesStatement valuesStatement){
        String trajectory_edge = getFileNameWithoutExtension(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH);

        //insert value into id_edge
        Column column_traj_name = new Column("traj_name");
        Column column_id = new Column("id");
        Column column_edges = new Column("edges");

        // 将上述的column加入将要insert的column中
        List<Column> insertColumns = new ArrayList<>();
        insertColumns.add(column_traj_name);
        insertColumns.add(column_id);
        insertColumns.add(column_edges);

        Insert insert = new Insert().withTable(new Table(trajectory_edge)).withColumns(insertColumns).withSelect(new Select().withSelectBody(valuesStatement));
        transaction.query(insert);
    }
    private void trajectoryVertexInsert(ValuesStatement valuesStatement){
        String trajectory_vertex = getFileNameWithoutExtension(setting.TRAJECTORY_VERTEX_REPRESENTATION_PATH);

        //insert value into id_edge
        Column column_traj_name = new Column("traj_name");
        Column column_id = new Column("id");
        Column column_vertexs = new Column("vertexs");

        // 将上述的column加入将要insert的column中
        List<Column> insertColumns = new ArrayList<>();
        insertColumns.add(column_traj_name);
        insertColumns.add(column_id);
        insertColumns.add(column_vertexs);

        Insert insert = new Insert().withTable(new Table(trajectory_vertex)).withColumns(insertColumns).withSelect(new Select().withSelectBody(valuesStatement));
        transaction.query(insert);
    }

    private void trajectoryTimePartialInsert(ValuesStatement valuesStatement)  {
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

}
