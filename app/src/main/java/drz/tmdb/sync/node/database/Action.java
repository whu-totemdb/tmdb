package drz.tmdb.sync.node.database;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.values.ValuesStatement;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;



//注意事项：
//1. className要为类的全部名称，即需要包括包的名称
//2. 三个String数组各个元素的位置是一一对应的
//3. 目前只支持基本的数据类型与String类，且建模类的构造函数的各参数类型需要是基本类型对应的类
//4. instantiation接口返回的是一个Object对象，实际上已经根据className成为了对应的类的实例化对象，获取后根据这个类进行转型即可

public class Action implements Serializable {
    private OperationType op;//操作类型

    private String schema;//数据库名称

    private String className;//类的名称，即表的名称

    //private int classID;//表的id，由下层存储进行分配，尚不清楚是否会使用到

    private long key;//主键，即tupleid

    private int attrNum;//操作的属性的个数

    private String[] attrName;//属性的名称

    private String[] attrType;//属性的类型，基本数据类型（如Integer、Long等）和String

    private String[] value;//属性的值



    public Action(OperationType op,
                  String schema,
                  String className,
                  //int classID,
                  long key,
                  int attrNum,
                  String[] attrName,
                  String[] attrType,
                  String[] value) {
        this.op = op;
        this.schema = schema;
        this.className = className;
        //this.classID = classID;
        this.key = key;
        this.attrNum = attrNum;
        this.attrName = attrName;
        this.attrType = attrType;
        this.value = value;
    }


    public OperationType getOp() {
        return op;
    }

    public void setOp(OperationType op) {
        this.op = op;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public int getAttrNum() {
        return attrNum;
    }

    public void setAttrNum(int attrNum) {
        this.attrNum = attrNum;
    }

    public String[] getAttrName() {
        return attrName;
    }

    public void setAttrName(String[] attrName) {
        this.attrName = attrName;
    }

    public String[] getAttrType() {
        return attrType;
    }

    public void setAttrType(String[] attrType) {
        this.attrType = attrType;
    }

    public String[] getValue() {
        return value;
    }

    public void setValue(String[] value) {
        this.value = value;
    }


    //基本数据类型包名全称
    public String getFullPacketPath(String typeName){
        String str;

        switch(typeName){
            case "char":
                str = "java.lang.Character";
                break;
            case "String":
                str = "java.lang.String";
                break;
            case "Integer":
                str = "java.lang.Integer";
                break;
            case "Long":
                str = "java.lang.Long";
                break;
            case "Short":
                str = "java.lang.Short";
                break;
            case "Boolean":
                str = "java.lang.Boolean";
                break;
            case "Float":
                str = "java.lang.Float";
                break;
            case "Double":
                str = "java.lang.Double";
                break;
            default:
                str = null;
                break;
        }

        return str;
    }

    //将每个值转化为对应的类型
    public Object[] cast(){
        int i;
        Object[] objs = new Object[attrNum];

        for(i = 0; i < attrNum; i++){
            switch (attrType[i]){
                /*case "char":
                    char c1 = value[i].charAt(0);
                    objs[i] = c1;
                    break;
                case "Char":
                    char c2 = value[i].charAt(0);
                    objs[i] = c2;
                    break;*/
                case "String":
                    String str = value[i];
                    objs[i] = str;
                    break;
                case "int":
                    int int2 = Integer.parseInt(value[i]);
                    objs[i] = int2;
                case "Integer":
                    int int1 = Integer.parseInt(value[i]);
                    objs[i] = int1;
                    break;
                case "Long":
                    long l = Long.parseLong(value[i]);
                    objs[i] = l;
                    break;
                case "long":
                    long l2 = Long.parseLong(value[i]);
                    objs[i] = l2;
                    break;
                case "boolean":
                    boolean b1 = Boolean.parseBoolean(value[i]);
                    objs[i] = b1;
                    break;
                case "Boolean":
                    boolean b2 = Boolean.parseBoolean(value[i]);
                    objs[i] = b2;
                    break;
                case "short":
                    short s1 = Short.parseShort(value[i]);
                    objs[i] = s1;
                    break;
                case "Short":
                    short s2 = Short.parseShort(value[i]);
                    objs[i] = s2;
                    break;
                case "float":
                    float f1 = Float.parseFloat(value[i]);
                    objs[i] = f1;
                    break;
                case "Float":
                    float f2 = Float.parseFloat(value[i]);
                    objs[i] = f2;
                    break;
                case "double":
                    double d1 = Double.parseDouble(value[i]);
                    objs[i] = d1;
                    break;
                case "Double":
                    double d2 = Double.parseDouble(value[i]);
                    objs[i] = d2;
                    break;
                default:
                    throw new RuntimeException("不属于基本数据类型");

            }
        }

        return objs;
    }


    //获取每个类型对应的class对象
    public Class<?>[] getAttrTypeClass(){
        Class<?>[] classes = new Class<?>[attrNum];

        for (int i = 0 ;i < attrNum; i++){
            try {
                String typeFullPacketName = getFullPacketPath(attrType[i]);
                classes[i] = Class.forName(typeFullPacketName);
            }catch (ClassNotFoundException e){
                e.printStackTrace();
            }

        }
        return classes;
    }

    //利用java的反射机制实现数据建模类对象的实例化
    public Object instantiation(String packageName) throws ClassNotFoundException,
            NoSuchMethodException,
            InstantiationException,
            IllegalAccessException,
            InvocationTargetException {


        Class<?> clazz = Class.forName(packageName + className);//className必须为全路径，即包的名称+类的名称

        Constructor<?> constructor = clazz.getDeclaredConstructor(getAttrTypeClass());
        Object[] o = cast();

        Object obj = constructor.newInstance(o);
        return obj;
    }

    //根据sql生成一个Action对象
    public static Action generate(String sqlStatement){
        Action action;
        OperationType op;
        String className;
        int attrNum;


        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sqlStatement.getBytes());
        try {
            Statement statement = CCJSqlParserUtil.parse(byteArrayInputStream);
            String operationType = statement.getClass().getSimpleName();
            switch (operationType){
                case "CreateTable":
                    op = OperationType.create;
                    CreateTable createTable = (CreateTable) statement;
                    className = createTable.getTable().getName();
                    break;
                case "Insert":
                    op = OperationType.insert;
                    Insert insert = (Insert) statement;
                    className = insert.getTable().getName();

                    List<Column> list = insert.getColumns();
                    attrNum = list.size();
                    String[] attrName = new String[attrNum];

                    int index = 0;
                    for (Column column : list){
                        attrName[index] = column.toString();
                        index++;
                    }

                    Select s = insert.getSelect();
                    String str = s.getSelectBody().getClass().getSimpleName();

                    MySelectVisitor selectVisitor = new MySelectVisitor();
                    s.getSelectBody().accept(selectVisitor);

                    return new Action(op,"",className,-1,selectVisitor.attrNum,attrName,selectVisitor.attrType,selectVisitor.value);

                case "Delete":
                    op = OperationType.delete;
                    Delete delete = (Delete) statement;
                    break;
                case "Update":
                    op = OperationType.update;
                    Update update = (Update) statement;
                    break;
                case "Select":
                    op = OperationType.select;
                    Select select = (Select) statement;

                    break;
                default:
                    break;
            }



        }catch (JSQLParserException e){
            e.printStackTrace();
        }

        return null;
        //return action;
    }



}
