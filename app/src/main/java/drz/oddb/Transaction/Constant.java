package drz.oddb.Transaction;

import drz.oddb.Transaction.SystemTable.BiPointerTableItem;
import drz.oddb.Transaction.SystemTable.ClassTableItem;
import drz.oddb.Transaction.SystemTable.DeputyTableItem;
import drz.oddb.Transaction.SystemTable.ObjectTableItem;
import drz.oddb.Transaction.SystemTable.SwitchingTableItem;

// 定义一些常量和静态方法
public class Constant {

    // 数据库文件目录
    public static final String DATABASE_DIR = "/data/data/drz.oddb/level/";

    // 允许占用内存的最大大小为1MB=1024*1024B，超过就会触发compact到外存
    public static final int MAX_MEM_SIZE = 1024 * 1024;

    // 允许单个data文件的最大大小为1MB=1024*1024B
    public static final int MAX_FILE_SIZE = 1024 * 1024;

    // 最小允许的文件占用比例，如果文件大小小于 maxSize * 这个数，则考虑将其合并以减少文件数量
    public static final float MIN_SIZE_USAGE_RATE = (float) 3 / 4;

    // 最大level数
    public static final int MAX_LEVEL = 3;

    // 允许各level的总大小 8MB 10MB 100MB 1000MB
    public static final int MAX_LEVEL0_SIZE = 8 * 1024 * 1024;
    public static final int MAX_LEVEL1_SIZE = 10 * 1024 * 1024;
    public static final int MAX_LEVEL2_SIZE = 100 * 1024 * 1024;
    public static final int MAX_LEVEL3_SIZE = 1000 * 1024 * 1024;
    public static final int[] MAX_LEVEL_SIZE = {MAX_LEVEL0_SIZE, MAX_LEVEL1_SIZE, MAX_LEVEL2_SIZE, MAX_LEVEL3_SIZE};

    // memTable写到SSTable时允许放到的最大level
    public static final int MAX_MEM_COMPACT_LEVEL = 2;

    // key 作为length允许占用的最大长度
    public static final int MAX_KEY_LENGTH = 10;

    // 编码key字符串为byte[]
    public static final byte[] KEY_TO_BYTES(String key){
        byte[] ret = new byte[Constant.MAX_KEY_LENGTH];
        byte[] temp = key.getBytes();
        if(temp.length <= Constant.MAX_KEY_LENGTH){
            for(int i=0;i<temp.length;i++){
                ret[i]=temp[i];
            }
            for(int i=temp.length;i<Constant.MAX_KEY_LENGTH;i++){
                // 不足的地方补全0
                ret[i]=(byte)32;
            }
        }
        return ret;
    }

    // 解码byte[]为key
    public static final String BYTES_TO_KEY(byte[] b){
        String s;
        int k=0;
        for(int i=0;i<Constant.MAX_KEY_LENGTH;i++){
            if(b[i]!=32){
                k++;
            }else{
                break;
            }
        }
        s=new String(b,0,k);
        return s;
    }

    // 编码int为byte
    public static byte[] INT_TO_BYTES(int value){
        int len = 4;
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[len - i - 1] = (byte)(value >> 8 * i);
        }
        return b;
    }

    // 解码byte为int
    public static int BYTES_TO_INT(byte[] b, int start, int len) {
        int sum = 0;
        int end = start + len;
        for (int i = start; i < end; i++) {
            int n = b[i]& 0xff;
            n <<= (--len) * 8;
            sum += n;
        }
        return sum;
    }

    // 根据一定规则给Object计算key
    public static String calculateKey(Object o){
        String key = "";
        if(o instanceof BiPointerTableItem){
            // 选择deputyobjectid作为BiPointerTableItem的key
            key = "b" + ((BiPointerTableItem) o).deputyobjectid;
        }else if(o instanceof ClassTableItem){
            // 选择classid作为ClassTableItem的key
            key = "c" + ((ClassTableItem) o).classid;
        }else if(o instanceof DeputyTableItem){
            // 选择deputyid作为DeputyTableItem的key
            key = "d" + ((DeputyTableItem) o).deputyid;
        }else if(o instanceof ObjectTableItem){
            // 选择tupleid作为ObjectTableItem的key
            key = "o" + ((ObjectTableItem) o).tupleid;
        }else if(o instanceof SwitchingTableItem){
            // SwitchingTableItem没有适合的key，只能通过拼接字符串的方式
            key = "s" + ((SwitchingTableItem) o).attr + "+" + ((SwitchingTableItem) o).deputy;
        }
        return key;

    }


    // 判断区间[a, b]  [c, d]是否有重叠
    // 如果b<c或者d<a则没有重叠
    public static boolean hasOverlap(String a, String b, String c, String d){
        if(b.compareTo(c)<= 0 || d.compareTo(a)<=0)
            return false;
        else
            return true;
    }



}
