package drz.tmdb.Level;

import static drz.tmdb.Level.Constant.DATABASE_DIR;

import com.alibaba.fastjson.JSONObject;

import org.apache.commons.lang3.ArrayUtils;
//import org.json.JSONObject;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import drz.tmdb.Transaction.SystemTable.BiPointerTableItem;
import drz.tmdb.Transaction.SystemTable.ClassTableItem;
import drz.tmdb.Transaction.SystemTable.DeputyTableItem;
import drz.tmdb.Transaction.SystemTable.ObjectTableItem;
import drz.tmdb.Transaction.SystemTable.SwitchingTableItem;

// 充当MemTable和SSTable的中间媒介
// 需要从SSTable中读取数据时（例如compaction），先读到一个FileData对象中
// 需要从内存写到SSTable时，也生成一个FileData对象，再调用writeSSTable()方法
public class FileData {

    // k-v
    // 使用SortedMap，自动按照key升序排序
    public TreeMap<String, Object> data = new TreeMap<>();

    // SSTable的文件名
    private String fileName;

    // 最大key与最小key
    private String maxKey = "";
    private String minKey = "";

    // BloomFilter
    private BloomFilter bloomFilter;

    // B树，记录每个data block的最大key的offset
    private BTree<String, Long> bTree = new BTree<>();

    public String getMaxKey() {
        return maxKey;
    }

    public String getMinKey() {
        return minKey;
    }

    // constructor
    // 将文件读到内存中
    // mode = 1 构造空的FileData对象，用于写文件
    // mode = 2 从SSTable读meta数据
    public FileData(String fileName, int mode){
        if(mode == 1){
            this.fileName = fileName;
        }
        if(mode == 2){
            this.fileName = fileName;
            // 读Footer
            long[] info = readFooter();
            long zoneMapOffset = info[0];
            long zoneMapLength = info[1];
            long bloomFilterOffset = info[2];
            long bloomFilterLength = info[3];
            long bTreeRootOffset = info[4];
            long indexBlockLength = info[5];
            // 读zone map
            readZoneMap(zoneMapOffset, zoneMapLength);
            // 初始化BloomFilter
            readBloomFilter(bloomFilterOffset, bloomFilterLength);
            // 初始化index block
            readIndexBlock(bTreeRootOffset, indexBlockLength);
        }
    }

    // 读Footer，返回的数据解析为6个long，分别对应zone map、bloom filter、index block的偏移和长度
    private long[] readFooter(){
        long[] ret = new long[6];
        try{
            File f = new File(Constant.DATABASE_DIR + this.fileName);
            FileInputStream input = new FileInputStream(f);
            // 移到文件末尾
            long fileLength = f.length();
            long startIndex = fileLength - 48;
            input.skip(startIndex);
            // 读取48B
            byte[] buffer = new byte[48];
            input.read(buffer, 0, 48);
            input.close();
            // 依次解析这6个long
            for(int i=0; i<6; i++){
                byte[] b = new byte[8];
                System.arraycopy(buffer, 8 * i, b, 0, 8);
                ret[i] = Constant.BYTES_TO_LONG(b);
            }
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    // 读zone map
    // zone map的格式：16字节先存minKey，16字节存maxKey
    private void readZoneMap(long offset, long length){
        byte[] buffer = Constant.readBytesFromFile(this.fileName, offset, (int) length);
        byte[] b1 = new byte[Constant.MAX_KEY_LENGTH];
        byte[] b2 = new byte[Constant.MAX_KEY_LENGTH];
        System.arraycopy(buffer, 0, b1, 0, Constant.MAX_KEY_LENGTH);
        System.arraycopy(buffer, Constant.MAX_KEY_LENGTH, b2, 0, Constant.MAX_KEY_LENGTH);
        this.minKey = Constant.BYTES_TO_KEY(b1);
        this.maxKey = Constant.BYTES_TO_KEY(b2);
    }

    // 读BloomFilter, 前4字节记录Bloom Filter的itemCount
    private void readBloomFilter(long offset, long length){
        // 通过BloomFilter的构造函数初始化
        this.bloomFilter = new BloomFilter(this.fileName, offset, (int) length);
    }

    // 读index block，并将各BTNode重新建成树
    private void readIndexBlock(long offset, long length){
        this.bTree = new BTree<>(this.fileName, offset);
    }

    // 将FileDate对象中的数据写到新SSTable中
    // 返回SSTable总字节数
    // 步骤：
    // 1. 分data block写入
    // 3. 写zone map
    // 4. 写Bloom filter
    // 5. 写index block
    // 6. 写Footer
    public long writeSSTable(){

        // 准备工作：初始化Bloom Filter，因为需要知道k-v的数量，因此放到此处初始化
        int itemCount = this.data.size();
        this.bloomFilter = new BloomFilter(itemCount);

        // 1. 分data block写入
        long dataBlockStartOffset = 0; // 此data block的开始偏移（记录B树结点时有用）
        long totalOffset = 0; // 总偏移
        StringBuilder writeIn = new StringBuilder();
        // 遍历所有k-v
        for(Entry<String, Object> entry : this.data.entrySet()){
            String key = entry.getKey();
            byte[] key_b = Constant.KEY_TO_BYTES(key);
            String value = JSONObject.toJSONString(entry.getValue());
            byte[] value_b = value.getBytes();
            // buffer为需要写入文件的字节数组，前4字节为该k-v的总长度
            byte[] buffer = new byte[Integer.BYTES + key_b.length + value_b.length];
            // buffer = length + key + value;
            System.arraycopy(Constant.INT_TO_BYTES(key_b.length + value_b.length), 0, buffer, 0, Integer.BYTES);
            System.arraycopy(key_b, 0, buffer, Integer.BYTES, key_b.length);
            System.arraycopy(value_b, 0, buffer, Integer.BYTES + key_b.length, value_b.length);
            // 为避免频繁调用接口写文件，现将buffer中的数据暂时记录，每次data block装满统一写入
            writeIn.append(new String(buffer));
            totalOffset += buffer.length;
            // 如果data block写满，则开启新data block，将旧data block的最大key和起始偏移记录到B树中
            if(totalOffset - dataBlockStartOffset > Constant.MAX_DATA_BLOCK_SIZE){
                this.bTree.insert(key, dataBlockStartOffset);
                Constant.writeBytesToFile(writeIn.toString().getBytes(), fileName);
                dataBlockStartOffset = totalOffset;
                writeIn = new StringBuilder("");
            }
            // k-v 写入SSTable
            //Constant.writeBytesToFile(buffer, this.fileName);
            // 更新Bloom Filter
            this.bloomFilter.add(key);
        }
        //  遍历结束时，未满的data block信息也写入B-Tree
        if(dataBlockStartOffset != totalOffset){
            this.bTree.insert(this.data.lastKey(), dataBlockStartOffset);
            Constant.writeBytesToFile(writeIn.toString().getBytes(), fileName);
        }

        // 3. 写zone map
        long zoneMapStartOffset = totalOffset; // zone map的开始偏移
        long zoneMapLength = Constant.MAX_KEY_LENGTH * 2; // zone map的长度
        this.minKey = this.data.firstKey();
        this.maxKey = this.data.lastKey();
        byte[] buffer = new byte[(int) zoneMapLength];
        System.arraycopy(Constant.KEY_TO_BYTES(this.minKey), 0, buffer, 0, Constant.MAX_KEY_LENGTH);
        System.arraycopy(Constant.KEY_TO_BYTES(this.maxKey), 0, buffer, Constant.MAX_KEY_LENGTH, Constant.MAX_KEY_LENGTH);
        Constant.writeBytesToFile(buffer, this.fileName);

        // 4. 写Bloom filter
        long bloomFilterStartOffset = zoneMapStartOffset + zoneMapLength;
        long bloomFilterLength = 4 + this.bloomFilter.getByteCount(); // +4 的原因见BloomFilter写文件的格式
        this.bloomFilter.writeToFile(this.fileName);

        // 5. 写index block
        long indexBlockStartOffset = bloomFilterStartOffset + bloomFilterLength;
        long[] info = this.bTree.write(this.fileName, indexBlockStartOffset);
        long indexBlockLength = info[0];
        long bTreeRootOffset = info[1];


        // 6. 写Footer
        long footerStartOffset = indexBlockStartOffset + indexBlockLength;
        long footerLength = Long.BYTES * 6;
        buffer = new byte[(int) footerLength];
        System.arraycopy(Constant.LONG_TO_BYTES(zoneMapStartOffset), 0, buffer, 0, Long.BYTES);
        System.arraycopy(Constant.LONG_TO_BYTES(zoneMapLength), 0, buffer, Long.BYTES, Long.BYTES);
        System.arraycopy(Constant.LONG_TO_BYTES(bloomFilterStartOffset), 0, buffer, Long.BYTES * 2, Long.BYTES);
        System.arraycopy(Constant.LONG_TO_BYTES(bloomFilterLength), 0, buffer, Long.BYTES * 3, Long.BYTES);
        System.arraycopy(Constant.LONG_TO_BYTES(bTreeRootOffset), 0, buffer, Long.BYTES * 4, Long.BYTES);
        System.arraycopy(Constant.LONG_TO_BYTES(indexBlockLength), 0, buffer, Long.BYTES * 5, Long.BYTES);
        Constant.writeBytesToFile(buffer, this.fileName);

        return footerStartOffset + footerLength;
    }

}
