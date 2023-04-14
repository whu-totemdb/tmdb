package drz.tmdb.Transaction.Transactions.impl;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import drz.tmdb.Memory.SystemTable.BiPointerTableItem;
import drz.tmdb.Memory.SystemTable.ClassTableItem;
import drz.tmdb.Memory.SystemTable.DeputyTableItem;
import drz.tmdb.Memory.SystemTable.ObjectTableItem;
import drz.tmdb.Memory.SystemTable.SwitchingTableItem;
import drz.tmdb.Memory.Tuple;
import drz.tmdb.Memory.TupleList;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.Insert;
import drz.tmdb.Transaction.Transactions.Select;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;
import drz.tmdb.Transaction.Transactions.utils.SelectResult;

public class InsertImpl implements Insert {
    MemConnect memConnect=new MemConnect();

    ArrayList<Integer> indexs=new ArrayList<>();

    public InsertImpl(MemConnect memConnect) {
        this.memConnect = memConnect;
    }

    public InsertImpl(){}

    public ArrayList<Integer> insert(Statement stmt) throws TMDBException {
        net.sf.jsqlparser.statement.insert.Insert statement=(net.sf.jsqlparser.statement.insert.Insert) stmt;
        //获取插入的table名
        Table table= statement.getTable();
        //获取插入的column的名称

        List<String> columns=new ArrayList<>();
        if(statement.getColumns()==null){
            columns=getColumns(table.getName());
        }
        else{
            for (int i = 0; i < statement.getColumns().size(); i++) {
                columns.add(statement.getColumns().get(i).getColumnName());
            }
        }
        //插入的TupleList
        SelectImpl select=new SelectImpl(memConnect);

        //insert后面的values是一个select节点，获取values或者其它类型select的值
        SelectResult selectResult=select.select(statement.getSelect());
        //tuplelist就是SelectResult中实际存储tuple部分
        TupleList tupleList=selectResult.getTpl();
        execute(table.getName(),columns,tupleList);
        return indexs;
    }

    public void execute(String table, List<String> columns, TupleList tupleList) throws TMDBException {
        int classId=memConnect.getClassId(table);
        int l=getLength(classId);
        int[] index=getTemplate(classId,columns);
        for (int i = 0; i < tupleList.tuplelist.size(); i++) {
            indexs.add(insert(classId,columns,tupleList.tuplelist.get(i),l,index));
        }
    }
    public void execute(int classId, List<String> columns, TupleList tupleList) throws TMDBException {
        int l=getLength(classId);
        int[] index=getTemplate(classId,columns);
        for (int i = 0; i < tupleList.tuplelist.size(); i++) {
            indexs.add(insert(classId,columns,tupleList.tuplelist.get(i),l,index));
        }
    }

    public int executeTuple(int classId, List<String> columns, Tuple tuple) throws TMDBException {
        int l=getLength(classId);
        int[] index=getTemplate(classId,columns);
        int insert = insert(classId, columns, tuple, l, index);
        indexs.add(insert);
        return insert;
    }


    /**
     * @param classId
     * @param columns
     * @param tuple
     * @param l
     * @param index
     * @return
     * @throws TMDBException
     */
    private Integer insert(int classId, List<String> columns, Tuple tuple, int l, int[] index) throws TMDBException {
        SelectImpl select=new SelectImpl(memConnect);
        int tupleid = memConnect.getTopt().maxTupleId++;
        Object[] temp=new Object[l];
        if(tuple.tuple.length!=columns.size()){
            throw new TMDBException("Insert error: columns size not equals to tuple size");
        }
        for (int i = 0; i < index.length; i++) {
            temp[index[i]]=tuple.tuple[i];
        }
        tuple.classId=classId;
        tuple.tuple=temp;
        tuple.tupleHeader=tuple.tuple.length;
        int[] ids=new int[tuple.tupleHeader];
        Arrays.fill(ids,tupleid);
        tuple.tupleIds=ids;
        tuple.tupleId=tupleid;
        memConnect.InsertTuple(tuple);
        memConnect.getTopt().objectTable.add(new ObjectTableItem(classId,tupleid));
        ArrayList<Integer> pointTo = deputySize(classId);
        ArrayList<HashMap<Integer, Integer>> deputyAttr = getDeputyAttr(pointTo.size(), classId);
        for (int i = 0; i < pointTo.size(); i++) {
            int tempClassId=pointTo.get(i);
            HashMap<String,String> tempMap=trans(deputyAttr.get(i),classId,tempClassId);
            List<String> tempColumns=getInsertColumns(classId,tempClassId,tempMap,columns);
            Tuple tuple1=getDeputyTuple(tempMap,tuple,columns);
            int i1 = executeTuple(tempClassId, tempColumns, tuple1);
            memConnect.getBiPointerT().biPointerTable.add(
                    new BiPointerTableItem(classId,tupleid,tempClassId,i1)
            );
        }

        //TODO TMDB
        LongestCommonSubSequence longestCommonSubSequence=new LongestCommonSubSequence();
        //现在由于多了基于轨迹相似度的代理类，因此，需要进行额外的逻辑处理

        //首先调用tJoinDeputySize方法得到是否当前类是否存在基于轨迹相似度的代理类，并得到代理类的id list

        //如果list非空就需要进一步判断

            //调用TrajTrans.getTraj方法将当前元祖的轨迹部分转化为List<Coordinate> traj1进行后续操作

            //对代理类idlist进行遍历

                //得到当前代理类的id

                //由于基于轨迹相似度的代理类需要两个源类，需要拿到另一个源类，这里使用getAnotherDeputy方法拿到

                //通过select的getTable（classid）方法拿到另一个源类的所有tuple

                //遍历另一个源类的所有tuple

                    //拿到另一个源类的当前tuple

                    //通过TrajTrans.getTraj方法将当前元祖的轨迹部分转化为List<Coordinate> traj2进行后续操作

                    //通过longestCommonSubSequence.getCommonSubsequence获取当前两个traj的公共子序列

                    //如果子序列长度大于阈值，则需要在代理类中插入新的tuple

                        //新建临时tuple，这个tuple就是要往代理类中进行插入的tuple
                        //临时tuple除了轨迹部分存的和当前进行插入的tuple（方法形参里的不同，其它都相同）步骤和TJoinSelect一样




                        //需要将得到的轨迹子序列，转换成string的形式，然后将tuple中轨迹部分设置为转换后的值


                        //调用一下方法在代理类中也插入新tuple
//                        int i1 = executeTuple(deputyId, columns, temp1);
//                        memConnect.getBiPointerT().biPointerTable.add(
//                                new BiPointerTableItem(classId, tupleid, deputyId, i1)





        return tupleid;
    }

    private int getAnotherDeputy(int deputyId, int classId) {
        for (int i = 0; i < memConnect.getDeputyt().deputyTable.size(); i++) {
            DeputyTableItem deputyTableItem = memConnect.getDeputyt().deputyTable.get(i);
            if(deputyTableItem.deputyrule[0].equals("5") &&
            deputyTableItem.originid!=classId &&
            deputyTableItem.deputyid==deputyId){
                return deputyTableItem.originid;
            }
        }
        return -1;
    }


    private HashMap<String, String> trans(HashMap<Integer, Integer> map, int classId, int tempClassId) {
        HashMap<String,String> map2=new HashMap<>();
        HashMap<Integer,String> tempmap1=new HashMap<>();
        HashMap<Integer,String> tempmap2=new HashMap<>();
        for (int i = 0; i < memConnect.getClasst().classTable.size(); i++) {
            ClassTableItem classTableItem = memConnect.getClasst().classTable.get(i);
            if(classTableItem.classid==classId){
                tempmap1.put(classTableItem.attrid,classTableItem.attrname);
            }
            if(classTableItem.classid==tempClassId){
                tempmap2.put(classTableItem.attrid,classTableItem.attrname);
            }
        }
        for(int i:map.keySet()){
            map2.put(tempmap1.get(i),tempmap2.get(map.get(i)));
        }
        return map2;
    }

    private Tuple getDeputyTuple(HashMap<String, String> map, Tuple tuple, List<String> columns) {
        Tuple res=new Tuple();
        Object[] temp=new Object[map.size()];
        int i=0;
        for(String s:columns){
            if (map.containsKey(s)) {
                temp[i]=tuple.tuple[columns.indexOf(s)];
                i++;
            }
        }
        res.tuple=temp;
        return res;
    }

    private List<String> getInsertColumns(int classId, int tempClassId, HashMap<String, String> map, List<String> columns) {

        List<String> res=new ArrayList<>();
        for (String column :
                columns) {
            if(map.containsKey(column)){
                res.add(map.get(column));
            }
        }
        return res;
    }

    public List<String> getColumns(String tableName){
        List<String> res=new ArrayList<>();
        for (int i = 0; i < memConnect.getClasst().classTable.size(); i++) {
            ClassTableItem classTableItem = memConnect.getClasst().classTable.get(i);
            if(classTableItem.classname.equals(tableName)){
                res.add(classTableItem.attrname);
            }
        }
        return res;
    }

    private ArrayList<HashMap<Integer,Integer>> getDeputyAttr(int deputySize, int oriId){
        int i=0;
        int c=-1;
        ArrayList<HashMap<Integer,Integer>> res=new ArrayList<>();
        while(i<memConnect.getSwitchingT().switchingTable.size()){
            SwitchingTableItem switchingTableItem = memConnect.getSwitchingT().switchingTable.get(i);
            if(switchingTableItem.oriId==oriId){
                c=switchingTableItem.deputyId;
                HashMap<Integer,Integer> map=new HashMap<>();
                while(i<memConnect.getSwitchingT().switchingTable.size() &&
                        memConnect.getSwitchingT().switchingTable.get(i).deputyId==c){
                    map.put(memConnect.getSwitchingT().switchingTable.get(i).oriAttrid,
                            memConnect.getSwitchingT().switchingTable.get(i).deputyAttrId);
                    i++;
                }
                res.add(map);
            }
            else{
                i++;
            }
        }
        return res;
    }

    //    public boolean executor()
//    public ArrayList<Integer> execute(net.sf.jsqlparser.statement.insert.Insert stmt) throws TMDBException {
////        ArrayList<ClassTableItem> classTableItems=memConnect.getSelectItem(table,columns);
//        //获取表名
//        Table table=stmt.getTable();
//        int l=getLength(table.getName());
//        //获取插入的column的名称
//        List<Column> columns=stmt.getColumns();
//        int[] index=getTemplate(table.getName(),columns);
//
//        Select select=new SelectImpl(memConnect);
//        //insert后面的values是一个select节点，获取values或者其它类型select的值
//        SelectResult selectResult=select.select(stmt.getSelect());
//        //tuplelist就是SelectResult中实际存储tuple部分
//        TupleList tupleList=selectResult.getTpl();
//        //获取插入表的表名
//        String tablename=table.getName();
//        ArrayList<Integer> integers = new ArrayList<>();
////        for(int i=0;i<tupleList.tuplenum;i++){
////            Tuple tuple=tupleList.tuplelist.get(i);
////            //创建memConnect中的tupleInsert需要的字符串数组
////            String[] p=new String[tuple.tuple.length+3];
////            p[0]="";
////            p[1]=""+tuple.tuple.length;
////            p[2]=tablename;
////            //循环的将每个values（或者其它类型的）的值调用memConnect进行插入
////            for(int j=0;j<tuple.tuple.length;j++){
////                p[j+3]=tuple.tuple[j].toString();
////            }
////            integers.add(tupleInsert(p));
////        }
//        return integers;
//    }

    private int getLength(int classId) {
        for (int i = 0; i < memConnect.getClasst().classTable.size(); i++) {
            if(memConnect.getClasst().classTable.get(i).classid==classId){
                return memConnect.getClasst().classTable.get(i).attrnum;
            }
        }
        try {
            throw  new TMDBException("Insert error: class doesn't exist");
        } catch (TMDBException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private int[] getTemplate(int classId, List<String> columns) {
        HashMap<Integer,ClassTableItem> map=new HashMap<>();
        ArrayList<ClassTableItem> list=new ArrayList<>();
        for (int j = 0; j < memConnect.getClasst().classTable.size(); j++) {
            ClassTableItem classTableItem = memConnect.getClasst().classTable.get(j);
            if(classTableItem.classid==classId){
                list.add(classTableItem);
            }
        }
        int[] res=new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            for (int j = 0; j < list.size(); j++) {
                ClassTableItem classTableItem = list.get(j);
                if(classTableItem.attrname.equals(columns.get(i))){
                    res[i]=classTableItem.attrid;
                    break;
                }
            }
        }
        return res;
    }

    private ArrayList<Integer> deputySize(int classId) {
        ArrayList<Integer> deputy=new ArrayList<>();
        for (int i = 0; i < memConnect.getDeputyt().deputyTable.size(); i++) {
            DeputyTableItem deputyTableItem = memConnect.getDeputyt().deputyTable.get(i);
            if(deputyTableItem.originid==classId && !deputyTableItem.deputyrule[0].equals("5")){
                deputy.add(deputyTableItem.deputyid);
            }
        }
        return deputy;
    }

    private ArrayList<Integer> tJoinDeputySize(int classId) {
        ArrayList<Integer> deputy=new ArrayList<>();
        for (int i = 0; i < memConnect.getDeputyt().deputyTable.size(); i++) {
            DeputyTableItem deputyTableItem = memConnect.getDeputyt().deputyTable.get(i);
            if(deputyTableItem.originid==classId && deputyTableItem.deputyrule[0].equals("5")){
                deputy.add(deputyTableItem.deputyid);
            }
        }
        return deputy;
    }

    public int tupleInsert(int classId, Tuple tuple, boolean hasDeputy){

        int tupleid = memConnect.getTopt().maxTupleId++;
        tuple.tupleHeader=tuple.tuple.length;
        int[] ids=new int[tuple.tupleHeader];
        Arrays.fill(ids,tupleid);
        tuple.tupleId=tupleid;
        memConnect.InsertTuple(tuple);
        memConnect.getTopt().objectTable.add(new ObjectTableItem(classId,tupleid));
        if(hasDeputy){

        }
        return 1;
    }

    //INSERT INTO aa VALUES (1,2,"3");
    //4,3,aa,1,2,"3"
    //0 1 2  3 4  5

    //INSERT INTO aa VALUES (1,2,"3");
    //4,3,aa,1,2,"3"
    //0 1 2  3 4  5
//    public int tupleInsert(String[] p) throws TMDBException {
//        int count = Integer.parseInt(p[1]);
//        for(int o =0;o<count+3;o++){
//            p[o] = p[o].replace("\"","");
//        }
//
//        String classname = p[2];
//        Object[] tuple_ = new Object[count];
//
//        int classid = -1;
//
//        for(ClassTableItem item: memConnect.getClasst().classTable)
//        {
//            if(item.classname.equals(classname)){
//                if(item.attrnum!=count) throw new TMDBException("插入参数长度不匹配实际参数长度");
//                classid = item.classid;
//                break;
//            }
//        }
//        if(classid==-1) throw new TMDBException("找不到"+classname);
//
//
//
//        for(int j = 0;j<count;j++){
//            tuple_[j] = p[j+3];
//        }
//
//        Tuple tuple = new Tuple(tuple_);
//        tuple.tupleHeader=count;
//        int tupleid = memConnect.getTopt().maxTupleId++;
//        tuple.tupleId=tupleid;
//        memConnect.InsertTuple(tuple);
//        memConnect.getTopt().objectTable.add(new ObjectTableItem(classid,tupleid));
//        //向代理类加元组
//        for(DeputyTableItem item: memConnect.getDeputyt().deputyTable){
//            if(classid == item.originid){
//                //判断代理规则
//
//                String attrtype=null;
//                int attrid=0;
//                for(ClassTableItem item1: memConnect.getClasst().classTable){
//                    if(item1.classid == classid&&item1.attrname.equals(item.deputyrule[0])) {
//                        attrtype = item1.attrtype;
//                        attrid = item1.attrid;
//                        break;
//                    }
//                }
//
//                if(memConnect.Condition(attrtype,tuple,attrid,item.deputyrule[2])){
//                    String[] ss= p.clone();
//                    String s1 = null;
//
//                    for(ClassTableItem item2:memConnect.getClasst().classTable){
//                        if(item2.classid == item.deputyid) {
//                            s1 = item2.classname;
//                            break;
//                        }
//                    }
//                    //是否要插switch的值
//                    //收集源类属性名
//                    String[] attrname1 = new String[count];
//                    int[] attrid1 = new int[count];
//                    int k=0;
//                    for(ClassTableItem item3 : memConnect.getClasst().classTable){
//                        if(item3.classid == classid){
//                            attrname1[k] = item3.attrname;
//                            attrid1[k] = item3.attrid;
//                            k++;
//
//                            if (k ==count)
//                                break;
//                        }
//                    }
//                    for (int l = 0;l<count;l++) {
//                        for (SwitchingTableItem item4 : memConnect.getSwitchingT().switchingTable) {
//                            if (item4.attr.equals(attrname1[l])){
//                                //判断被置换的属性是否是代理类的
//
//                                for(ClassTableItem item8: memConnect.getClasst().classTable){
//                                    if(item8.attrname.equals(item4.deputy)&&Integer.parseInt(item4.rule)!=0){
//                                        if(item8.classid==item.deputyid){
//                                            int sw = Integer.parseInt(p[3+attrid1[l]]);
//                                            ss[3+attrid1[l]] = new Integer(sw+Integer.parseInt(item4.rule)).toString();
//                                            break;
//                                        }
//                                    }
//                                }
//
//
//                            }
//                        }
//                    }
//
//                    ss[2] = s1;
//                    int deojid=tupleInsert(ss);
//                    //插入Bi
//                    memConnect.getBiPointerT().biPointerTable.add(new BiPointerTableItem(classid,tupleid,item.deputyid,deojid));
//                }
//            }
//        }
//        return tupleid;
//    }
}
