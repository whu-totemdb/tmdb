package edu.whu.tmdb.storage.level;

//import org.json.JSONObject;

import edu.whu.tmdb.storage.level.BTree;
import edu.whu.tmdb.storage.utils.Constant;
import edu.whu.tmdb.storage.utils.K;
import edu.whu.tmdb.storage.utils.V;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

// 内存中的SSTable
// 通过构造方法SSTable()将数据从磁盘读到内存
// 通过writeSSTable()将数据从内存写到磁盘
public class SSTable {

    // k-v
    // 使用SortedMap，自动按照key升序排序
    public TreeMap<K, V> data = new TreeMap<>();

    // SSTable的文件名
    public String fileName;

    // 最大key与最小key
    public K maxKey = new K();
    K minKey = new K();

    // BloomFilter
    public BloomFilter bloomFilter;

    // B树，记录每个data block的最大key的offset
    BTree bTree;

    // SSTable的写通道
    // 为避免频繁new flush close outputStream而浪费大量时间，将其设置成类属性一次打开一次关闭
    BufferedOutputStream outputStream;

    // SSTable的读通道
    // 由于读SSTable时不需要从头开始，多为根据指定offset跳着读，因此使用RandomAccessFile更快
    public RandomAccessFile raf;

    // search时确定扫描上界
    public long zoneMapOffset;

    public K getMaxKey() {
        return maxKey;
    }

    public K getMinKey() {
        return minKey;
    }

    // constructor
    // 将文件读到内存中
    // mode = 1 构造空的SSTable对象，并初始化写通道
    // mode = 2 构造空的SSTable对象，并初始化读通道
    // mode = 3 从SSTable读整个元数据库（示范）
    public SSTable(String fileName, int mode){
        if(mode == 1){
            this.fileName = fileName;
            this.bTree = new BTree(3);
            // 初始化写通道
            try{
                File f = new File(Constant.DATABASE_DIR + this.fileName);
                if(!f.exists())
                    f.createNewFile();
                this.outputStream = new BufferedOutputStream(new FileOutputStream(f, true));
            } catch (IOException e){
                e.printStackTrace();
            }
        }
        else if(mode == 2){
            this.fileName = fileName;
            // 初始化读通道
            try{
                File f = new File(Constant.DATABASE_DIR + this.fileName);
                this.raf = new RandomAccessFile(f, "r");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }else if(mode == 3){
            this.fileName = fileName;
            // 初始化读通道
            try{
                File f = new File(Constant.DATABASE_DIR + this.fileName);
                this.raf = new RandomAccessFile(f, "r");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            // 读Footer
            long[] info = readFooter();
            long zoneMapOffset = info[0];
            long zoneMapLength = info[1];
            this.zoneMapOffset = zoneMapOffset;
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


    // 析构函数，关闭SSTable的读、写通道
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if(outputStream != null){
            outputStream.flush();
            outputStream.close();
        }
        if(raf != null){
            raf.close();
        }

    }

    // 向此SSTable末尾追加写字节数组data
    void appendToFile(byte[] data){
        try{
            this.outputStream.write(data, 0, data.length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 从此SSTable偏移为offset处读取长度为length的字节数组
    private byte[] readFromFile(long offset, int length){
        byte[] ret = new byte[length];
        try{
            raf.seek(offset);
            raf.read(ret);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    // 读Footer，返回的数据解析为6个long，分别对应zone map、bloom filter、index block的偏移和长度
    public long[] readFooter(){
        long[] ret = new long[6];
        File f = new File(Constant.DATABASE_DIR + this.fileName);
        long offset = f.length() - 6 * Long.BYTES;  // 开始读的偏移
        byte[] buffer = readFromFile(offset, 6 * Long.BYTES);
        // 依次解析这6个long
        for(int i=0; i<6; i++){
            byte[] b = new byte[8];
            System.arraycopy(buffer, 8 * i, b, 0, 8);
            ret[i] = Constant.BYTES_TO_LONG(b);
        }
        return ret;
    }

    // 读zone map
    // zone map的格式：16字节先存minKey，16字节存maxKey
    public void readZoneMap(long offset, long length){
        byte[] buffer = readFromFile(offset, (int) length);
        byte[] b1 = new byte[Constant.MAX_KEY_LENGTH];
        byte[] b2 = new byte[Constant.MAX_KEY_LENGTH];
        System.arraycopy(buffer, 0, b1, 0, Constant.MAX_KEY_LENGTH);
        System.arraycopy(buffer, Constant.MAX_KEY_LENGTH, b2, 0, Constant.MAX_KEY_LENGTH);
        this.minKey = new K(b1);
        this.maxKey = new K(b2);
    }

    // 读BloomFilter, 前4字节记录Bloom Filter的itemCount
    private void readBloomFilter(long offset, long length){
        // 通过BloomFilter的构造函数初始化
        this.bloomFilter = new BloomFilter(this.raf, offset, (int) length);
    }

    // 读index block，并将各BTNode重新建成树
    public void readIndexBlock(long offset, long length){
        this.bTree = new BTree(this.raf, offset);
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
        this.bTree = new BTree(3);

        // 1. 分data block写入
        long dataBlockStartOffset = 0; // 此data block的开始偏移（记录B树结点时有用）
        long totalOffset = 0; // 总偏移
        K lastKey = new K(); // 记录上一个key
        // 遍历所有k-v
        for(Entry<K, V> entry : this.data.entrySet()){
            K key = entry.getKey();
            byte[] key_b = key.serialize();
            V value = entry.getValue();
            byte[] value_b = value.serialize();

            // 更新Bloom Filter
            this.bloomFilter.add(key);

            // 此kv占用总字节数
            long len = Integer.BYTES + key_b.length + value_b.length;

            // 如果加上此kv则data block写满，则开启新data block，将旧data block的最大key和起始偏移记录到B树中
            if(totalOffset - dataBlockStartOffset + len > Constant.MAX_DATA_BLOCK_SIZE){
                this.bTree.insert(lastKey, dataBlockStartOffset);
                // 此data block剩余部分使用0填满
                int vacant = (int) (Constant.MAX_DATA_BLOCK_SIZE + dataBlockStartOffset - totalOffset);
                byte[] buffer0 = new byte[vacant];
                appendToFile(buffer0);
                totalOffset += vacant;
                dataBlockStartOffset = totalOffset;
            }

            // 写入 length + key + value;
            appendToFile(Constant.INT_TO_BYTES(key_b.length + value_b.length));
            appendToFile(key_b);
            appendToFile(value_b);
            totalOffset += len;

            lastKey = key;

        }
        //  遍历结束时，未满的data block信息也写入B-Tree
        if(dataBlockStartOffset != totalOffset){
            this.bTree.insert(this.data.lastKey(), dataBlockStartOffset);
            // 此data block剩余部分使用0填满
            int vacant = (int) (Constant.MAX_DATA_BLOCK_SIZE + dataBlockStartOffset - totalOffset);
            byte[] buffer0 = new byte[vacant];
            appendToFile(buffer0);
            totalOffset += vacant;
            dataBlockStartOffset = totalOffset;
        }

        // 3. 写zone map
        long zoneMapStartOffset = totalOffset; // zone map的开始偏移
        long zoneMapLength = Constant.MAX_KEY_LENGTH * 2; // zone map的长度
        this.zoneMapOffset = zoneMapStartOffset;
        this.minKey = this.data.firstKey();
        this.maxKey = this.data.lastKey();
        byte[] buffer = new byte[(int) zoneMapLength];
        System.arraycopy(this.minKey.serialize(), 0, buffer, 0, Constant.MAX_KEY_LENGTH);
        System.arraycopy(this.maxKey.serialize(), 0, buffer, Constant.MAX_KEY_LENGTH, Constant.MAX_KEY_LENGTH);
        appendToFile(buffer);

        // 4. 写Bloom filter
        long bloomFilterStartOffset = zoneMapStartOffset + zoneMapLength;
        long bloomFilterLength = 4 + this.bloomFilter.getByteCount(); // +4 的原因见BloomFilter写文件的格式
        this.bloomFilter.writeToFile(this.outputStream);

        // 5. 写index block
        long indexBlockStartOffset = bloomFilterStartOffset + bloomFilterLength;
        long[] info = this.bTree.write(this.outputStream, indexBlockStartOffset);
        long indexBlockLength = info[0];
        long bTreeRootOffset = info[1];


        // 6. 写Footer
        long footerStartOffset = indexBlockStartOffset + indexBlockLength;
        long footerLength = Long.BYTES * 6;
        appendToFile(Constant.LONG_TO_BYTES(zoneMapStartOffset));
        appendToFile(Constant.LONG_TO_BYTES(zoneMapLength));
        appendToFile(Constant.LONG_TO_BYTES(bloomFilterStartOffset));
        appendToFile(Constant.LONG_TO_BYTES(bloomFilterLength));
        appendToFile(Constant.LONG_TO_BYTES(bTreeRootOffset));
        appendToFile(Constant.LONG_TO_BYTES(indexBlockLength));

        // flush
        try{
            this.outputStream.flush();
        }catch (IOException e){
            e.printStackTrace();
        }

        return footerStartOffset + footerLength;
    }


    // 在单个SSTable中查
    // 根据zone map查询如果不在范围中则返回null
    // 根据bloom filter查或者遍历查询没找到则返回""
    public V search(K key) throws IOException {

        // 如果meta data不完整，则需要从文件中读取
        if(this.maxKey.equals(new K())){
            // 读Footer
            long[] info = readFooter();
            long zoneMapOffset = info[0];
            long zoneMapLength = info[1];
            this.zoneMapOffset = zoneMapOffset;
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


        // 1. 检查zone map
        if(key.compareTo(this.minKey) < 0 || key.compareTo(this.maxKey) > 0)
            return null;

        // 2. 检查bloom filter
        if(!this.bloomFilter.check(key))
            return new V();

        // 如果1 2 均通过，说明key极有可能存在该SSTable中
        // 3. 定位到该key可能存在的data block
        Long offset = this.bTree.leftSearch(key);
        if(offset == null)
            offset = 0l;

        // 4. 读该data block
        byte[] dataBlock = new byte[(int) Constant.MAX_DATA_BLOCK_SIZE];
        this.raf.seek(offset);
        this.raf.read(dataBlock);

        // 5. 遍历data block
        byte[] targetKeyBuffer = key.serialize();
        int currentOffset = 0; // 记录当前在data block中的位置
        int length; // 一条kv记录的长度
        while(currentOffset + Integer.BYTES < Constant.MAX_DATA_BLOCK_SIZE){
            length = Constant.BYTES_TO_INT(dataBlock, currentOffset, Integer.BYTES);
            currentOffset += Integer.BYTES;
            if(length == 0)
                // length = 0说明读取的内容是补0的data block的空余部分
                break;

            // 判断k是否匹配，如果匹配则返回
            if(Constant.compareArray(dataBlock, currentOffset, targetKeyBuffer, 0, Constant.MAX_KEY_LENGTH)){
                byte[] vBuffer = new byte[length - Constant.MAX_KEY_LENGTH];
                System.arraycopy(dataBlock, currentOffset + Constant.MAX_KEY_LENGTH, vBuffer, 0, vBuffer.length);
                return new V(vBuffer);
            }
            currentOffset += length;
        }

        return new V();
    }


    // 在SSTable中进行rangeQuery
    public Map<K, V> rangeQuery(K startKey, K endKey) throws IOException {

        // 如果meta data不完整，则需要从文件中读取
        if(this.maxKey.equals(new K())){
            // 读Footer
            long[] info = readFooter();
            long zoneMapOffset = info[0];
            long zoneMapLength = info[1];
            this.zoneMapOffset = zoneMapOffset;
            long bTreeRootOffset = info[4];
            long indexBlockLength = info[5];

            // 读zone map
            readZoneMap(zoneMapOffset, zoneMapLength);

            // 初始化index block
            readIndexBlock(bTreeRootOffset, indexBlockLength);
        }

        Map<K, V> result = new TreeMap<>();

        // 1. 检查zone map
        if(startKey.compareTo(this.maxKey) > 0 || endKey.compareTo(this.minKey) <= 0)
            return result;

        // 2. 通过index block 定位到startkey所在的data block
        Long startOffset = this.bTree.leftSearch(startKey);
        if(startOffset == null)
            startOffset = 0l;

        // 3. 确定扫描的上界
        Long endOffset = this.bTree.leftSearch(endKey);
        if(endOffset == null)
            endOffset = 0l;
        endOffset += Constant.MAX_DATA_BLOCK_SIZE;


        // 4. 开始扫描
        this.raf.seek(startOffset); // 首先定位到开始偏移
        byte[] dataBlock = new byte[Constant.MAX_DATA_BLOCK_SIZE];
        int dataBlockCount = (int) ((endOffset - startOffset) / Constant.MAX_DATA_BLOCK_SIZE); // 需要读取的data block数量
        byte[] keyBuffer = new byte[Constant.MAX_KEY_LENGTH];
        byte[] valueBuffer;
        for(int i=0; i<dataBlockCount; i++){
            raf.read(dataBlock); // 以data block为单位进行读取

            int currentOffset = 0; // 指示当前data block的扫描进度
            while(currentOffset + Integer.BYTES < Constant.MAX_DATA_BLOCK_SIZE){
                int length = Constant.BYTES_TO_INT(dataBlock, currentOffset, Integer.BYTES); // 读kv总长度
                currentOffset += Integer.BYTES;
                if(length == 0)
                    // length = 0说明读取的内容是补0的data block的空余部分，进入下一个data block
                    continue;


                // 加载key
                System.arraycopy(dataBlock, currentOffset, keyBuffer, 0, Constant.MAX_KEY_LENGTH);
                K k = new K(keyBuffer);

                // 如果小于startKey，扫描下一个键值对
                if(k.compareTo(startKey) < 0){
                    currentOffset += length;
                    continue;
                }

                // 如果大于endKey，结束
                if(endKey.compareTo(k) < 0)
                    break;

                // 否则，加入返回结果集
                valueBuffer = new byte[length - Constant.MAX_KEY_LENGTH];
                System.arraycopy(dataBlock, currentOffset + Constant.MAX_KEY_LENGTH, valueBuffer, 0, valueBuffer.length);
                V v = new V(valueBuffer);
                result.put(k, v);

                currentOffset += length;
            }
        }


        return result;
    }

}
