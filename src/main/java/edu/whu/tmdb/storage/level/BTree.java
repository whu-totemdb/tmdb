package edu.whu.tmdb.storage.level;


import edu.whu.tmdb.storage.utils.Constant;
import edu.whu.tmdb.storage.utils.K;
import edu.whu.tmdb.storage.utils.V;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * 一颗B树的简单实现。
 */
@SuppressWarnings("all")
public class BTree {
    //private static Log logger = LogFactory.getLog(BTree.class);

    /**
     * B树节点中的键值对。
     * <p/>
     * B树的节点中存储的是键值对。
     * 通过键访问值。
     */
    private static class Entry {
        private K key;
        private long value;

        public Entry(K k, long v) {
            this.key = k;
            this.value = v;
        }

        public K getKey() {
            return key;
        }

        public long getValue() {
            return value;
        }

        public void setValue(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return key + ":" + value;
        }
    }

    /**
     * 在B树节点中搜索给定键值的返回结果。
     * <p/>
     * 该结果有两部分组成。第一部分表示此次查找是否成功，
     * 如果查找成功，第二部分表示给定键值在B树节点中的位置，
     * 如果查找失败，第二部分表示给定键值应该插入的位置。
     */
    private static class SearchResult<V> {
        private boolean exist;
        private int index;
        private long value;

        public SearchResult(boolean exist, int index) {
            this.exist = exist;
            this.index = index;
        }

        public SearchResult(boolean exist, int index, long value) {
            this(exist, index);
            this.value = value;
        }

        public boolean isExist() {
            return exist;
        }

        public int getIndex() {
            return index;
        }

        public long getValue() {
            return value;
        }
    }

    /**
     * B树中的节点。
     * <p>
     */
    private static class BTreeNode {
        /**
         * 节点的项，按键非降序存放
         */
        private List<Entry> entrys;
        /**
         * 内节点的子节点
         */
        private List<BTreeNode> children;
        /**
         * 是否为叶子节点
         */
        private boolean leaf;
        /**
         * 键的比较函数对象
         */
        private Comparator<K> kComparator;

        // 记录该节点在文件中的偏移
        private long offset;

        private BTreeNode() {
            entrys = new ArrayList<Entry>();
            children = new ArrayList<BTreeNode>();
            leaf = false;
        }

        public BTreeNode(Comparator<K> kComparator) {
            this();
            this.kComparator = kComparator;
        }

        // 构造函数，从文件fileName的offset偏移处读取节点，并递归读取子节点
        // 单个BTNode的存储格式：
        // type       int, 0表示非叶子节点，1表示叶子节点
        // k-v        首先用一个int表示有多少个键值对，k长度在Constant文件中有限制，v为long
        // children   type=1时不存在。首先用一个int表示有多少个子节点，每个子节点用一个long记录偏移
        public BTreeNode(RandomAccessFile raf, long offset){
            entrys = new ArrayList<Entry>();
            children = new ArrayList<BTreeNode>();

            // 首先读4字节判断节点的type
            int type = Constant.BYTES_TO_INT(Constant.readBytesFromFile(raf, offset, 4), 0, 4);
            offset += 4;

            // 叶子节点
            if(type == 1){
                leaf = true;
                // 读4字节即一个int，得到有多少个entry
                int entryCount = Constant.BYTES_TO_INT(Constant.readBytesFromFile(raf, offset, 4), 0, 4);
                offset += 4;
                // 每个Entry即每个k-v占用的字节数
                int singleEntryLength = Constant.MAX_KEY_LENGTH + Long.BYTES;
                // 总占用字节数
                int totalLength = entryCount * singleEntryLength;
                // 读取entryCount个Entry
                byte[] buffer = Constant.readBytesFromFile(raf, offset, totalLength);
                offset += totalLength;
                // 构造entryCount个Entry
                for(int i=0; i<entryCount; i++){
                    byte[] b;
                    // K
                    b = new byte[Constant.MAX_KEY_LENGTH];
                    System.arraycopy(buffer, singleEntryLength * i, b, 0, Constant.MAX_KEY_LENGTH);
                    K k = new K(b);
                    // V
                    b = new byte[Long.BYTES];
                    System.arraycopy(buffer, singleEntryLength * i + Constant.MAX_KEY_LENGTH, b, 0, Long.BYTES);
                    long v = Constant.BYTES_TO_LONG(b);
                    // 添加entry
                    entrys.add((Entry) new Entry(k, v));
                }
                return;
            }
            // 非叶子节点
            else{
                leaf = false;

                // 后根遍历，先读子节点
                // 1. 计算子节点所在的偏移
                long entryStartOffset = offset; // 记录Entry开始的offset
                // 读4字节即一个int，得到有多少个entry
                int entryCount = Constant.BYTES_TO_INT(Constant.readBytesFromFile(raf, entryStartOffset, 4), 0, 4);
                // 每个Entry即每个k-v占用的字节数
                int singleEntryLength = Constant.MAX_KEY_LENGTH + Long.BYTES;
                // Entry总占用字节数
                int totalEntryLength = entryCount * singleEntryLength;
                long childrenStartOffset = offset + 4 + totalEntryLength; // 记录children开始的offset
                // 读4字节即一个int，得到有多少个child
                int childrenCount = Constant.BYTES_TO_INT(Constant.readBytesFromFile(raf, childrenStartOffset, 4), 0, 4);
                // 每个child占用一个long 8字节，共占用
                int totalChildrenLength = childrenCount * Long.BYTES;

                // 2.读取childrenCount个child
                byte[] buffer = Constant.readBytesFromFile(raf, childrenStartOffset + 4, totalChildrenLength);
                // 递归构造childrenCount个child
                for(int i=0; i<childrenCount; i++){
                    // 读取long为孩子节点的地址
                    byte[] b = new byte[Long.BYTES];
                    System.arraycopy(buffer, Long.BYTES * i, b, 0, Long.BYTES);
                    long address = Constant.BYTES_TO_LONG(b);
                    // 递归
                    children.add(new BTreeNode(raf, address));
                }

                // 读取entryCount个Entry
                buffer = Constant.readBytesFromFile(raf, entryStartOffset + 4, totalEntryLength);
                // 构造entryCount个Entry
                for(int i=0; i<entryCount; i++){
                    byte[] b;
                    // K
                    b = new byte[Constant.MAX_KEY_LENGTH];
                    System.arraycopy(buffer, singleEntryLength * i, b, 0, Constant.MAX_KEY_LENGTH);
                    K k = new K(b);
                    // V
                    b = new byte[Long.BYTES];
                    System.arraycopy(buffer, singleEntryLength * i + Constant.MAX_KEY_LENGTH, b, 0, Long.BYTES);
                    long v = Constant.BYTES_TO_LONG(b);
                    // 添加entry
                    entrys.add(new Entry(k, v));
                }
                return;
            }

        }

        // 可以参考二叉搜索树的策略。
        // key为目标key
        // record记录该层结点的返回值，如果子节点中没有找到更符合条件（即更大）的key时，返回该record
        private long leftSearch(K key, Entry record){

            // 遍历当前结点的entry，找到比他小的最大值
            int n = this.entrys.size();
            int i = 0;
            while(i < n){
                // 如果恰好相等，直接返回
                if(compare(this.entrys.get(i).getKey(), key) == 0)
                    return this.entrys.get(i).getValue();
                if(compare(this.entrys.get(i).getKey(), key) < 0)
                    i++;
                else
                    break;;
            }
            if(i < this.entrys.size())
                record = this.entrys.get(i);

            // 如果是叶子节点
            // 比较该层的record和上层的record，选择更大的返回
            if(this.isLeaf()){
                if(i >= this.entrys.size())
                    return record.getValue();
                if(compare(this.entrys.get(i).getKey(), record.getKey()) < 0)
                    return record.getValue();
                else
                    return this.entrys.get(i).getValue();
            }

            // 否则递归到子节点中查找
            return this.children.get(i).leftSearch(key, record);
        }


        public boolean isLeaf() {
            return leaf;
        }

        public void setLeaf(boolean leaf) {
            this.leaf = leaf;
        }

        /**
         * 返回项的个数。如果是非叶子节点，根据B树的定义，
         * 该节点的子节点个数为({@link #size()} + 1)。
         *
         * @return 关键字的个数
         */
        public int size() {
            return entrys.size();
        }

        int compare(K key1, K key2) {
            return kComparator == null ? ((Comparable<K>) key1).compareTo(key2) : kComparator.compare(key1, key2);
        }

        /**
         * 在节点中查找给定的键。
         * 如果节点中存在给定的键，则返回一个<code>SearchResult</code>，
         * 标识此次查找成功，给定的键在节点中的索引和给定的键关联的值；
         * 如果不存在，则返回<code>SearchResult</code>，
         * 标识此次查找失败，给定的键应该插入的位置，该键的关联值为null。
         * <p/>
         * 如果查找失败，返回结果中的索引域为[0, {@link #size()}]；
         * 如果查找成功，返回结果中的索引域为[0, {@link #size()} - 1]
         * <p/>
         * 这是一个二分查找算法，可以保证时间复杂度为O(log(t))。
         *
         * @param key - 给定的键值
         * @return - 查找结果
         */
        public SearchResult searchKey(K key) {
            int low = 0;
            int high = entrys.size() - 1;
            int mid = 0;
            while (low <= high) {
                mid = (low + high) / 2; // 先这么写吧，BTree实现中，l+h不可能溢出
                Entry entry = entrys.get(mid);
                if (compare(entry.getKey(), key) == 0) // entrys.get(mid).getKey() == key
                    break;
                else if (compare(entry.getKey(), key) > 0) // entrys.get(mid).getKey() > key
                    high = mid - 1;
                else // entry.get(mid).getKey() < key
                    low = mid + 1;
            }
            boolean result = false;
            int index = 0;
            long value = 0;
            if (low <= high) // 说明查找成功
            {
                result = true;
                index = mid; // index表示元素所在的位置
                value = entrys.get(index).getValue();
            } else {
                result = false;
                index = low; // index表示元素应该插入的位置
            }
            return new SearchResult<V>(result, index, value);
        }

        /**
         * 将给定的项追加到节点的末尾，
         * 你需要自己确保调用该方法之后，节点中的项还是
         * 按照关键字以非降序存放。
         *
         * @param entry - 给定的项
         */
        public void addEntry(Entry entry) {
            entrys.add(entry);
        }

        /**
         * 删除给定索引的<code>entry</code>。
         * <p/>
         * 你需要自己保证给定的索引是合法的。
         *
         * @param index - 给定的索引
         */
        public Entry removeEntry(int index) {
            return entrys.remove(index);
        }

        /**
         * 得到节点中给定索引的项。
         * <p/>
         * 你需要自己保证给定的索引是合法的。
         *
         * @param index - 给定的索引
         * @return 节点中给定索引的项
         */
        public Entry entryAt(int index) {
            return entrys.get(index);
        }

        /**
         * 如果节点中存在给定的键，则更新其关联的值。
         * 否则插入。
         *
         * @param entry - 给定的项
         * @return null，如果节点之前不存在给定的键，否则返回给定键之前关联的值
         */
        public long putEntry(Entry entry) {
            SearchResult<V> result = searchKey(entry.getKey());
            if (result.isExist()) {
                long oldValue = entrys.get(result.getIndex()).getValue();
                entrys.get(result.getIndex()).setValue(entry.getValue());
                return oldValue;
            } else {
                insertEntry(entry, result.getIndex());
                return 0;
            }
        }

        /**
         * 在该节点中插入给定的项，
         * 该方法保证插入之后，其键值还是以非降序存放。
         * <p/>
         * 不过该方法的时间复杂度为O(t)。
         * <p/>
         * <b>注意：</b>B树中不允许键值重复。
         *
         * @param entry - 给定的键值
         * @return true，如果插入成功，false，如果插入失败
         */
        public boolean insertEntry(Entry entry) {
            SearchResult<V> result = searchKey(entry.getKey());
            if (result.isExist())
                return false;
            else {
                insertEntry(entry, result.getIndex());
                return true;
            }
        }

        /**
         * 在该节点中给定索引的位置插入给定的项，
         * 你需要自己保证项插入了正确的位置。
         *
         * @param entry - 给定的键值
         * @param index - 给定的索引
         */
        public void insertEntry(Entry entry, int index) {
            /*
             * 通过新建一个ArrayList来实现插入真的很恶心，先这样吧
             * 要是有类似C中的reallocate就好了。
             */
            List<Entry> newEntrys = new ArrayList<Entry>();
            int i = 0;
            // index = 0或者index = keys.size()都没有问题
            for (; i < index; ++i)
                newEntrys.add(entrys.get(i));
            newEntrys.add(entry);
            for (; i < entrys.size(); ++i)
                newEntrys.add(entrys.get(i));
            entrys.clear();
            entrys = newEntrys;
        }

        /**
         * 返回节点中给定索引的子节点。
         * <p/>
         * 你需要自己保证给定的索引是合法的。
         *
         * @param index - 给定的索引
         * @return 给定索引对应的子节点
         */
        public BTreeNode childAt(int index) {
            if (isLeaf())
                throw new UnsupportedOperationException("Leaf node doesn't have children.");
            return children.get(index);
        }

        /**
         * 将给定的子节点追加到该节点的末尾。
         *
         * @param child - 给定的子节点
         */
        public void addChild(BTreeNode child) {
            children.add(child);
        }

        /**
         * 删除该节点中给定索引位置的子节点。
         * </p>
         * 你需要自己保证给定的索引是合法的。
         *
         * @param index - 给定的索引
         */
        public void removeChild(int index) {
            children.remove(index);
        }

        /**
         * 将给定的子节点插入到该节点中给定索引
         * 的位置。
         *
         * @param child - 给定的子节点
         * @param index - 子节点带插入的位置
         */
        public void insertChild(BTreeNode child, int index) {
            List<BTreeNode> newChildren = new ArrayList<BTreeNode>();
            int i = 0;
            for (; i < index; ++i)
                newChildren.add(children.get(i));
            newChildren.add(child);
            for (; i < children.size(); ++i)
                newChildren.add(children.get(i));
            children = newChildren;
        }


        // BTNodes的持久化保存, 写到fileName偏移为offset处，返回值为该节点的长度
        // 单个BTNode的存储格式：
        // type       int, 0表示非叶子节点，1表示叶子节点
        // k-v        首先用一个int表示有多少个键值对，k长度在Constant文件中有限制，v为long
        // children   type=1时不存在。首先用一个int表示有多少个子节点，每个子节点用一个long记录偏移
        public long write(BufferedOutputStream outputStream, long offset) throws IOException {
            // 更新offset
            this.offset = offset;

            // 叶子节点
            if(leaf){
                // 更新offset
                this.offset = offset;
                // 每个Entry即每个k-v占用的字节数
                int singleEntryLength = Constant.MAX_KEY_LENGTH + Long.BYTES;
                // 该节点所有Entry需要占用的字节数
                // 额外的8个字节，一个字节记录该node的type，一个字节记录k-v对的个数
                int totalLength = entrys.size() * singleEntryLength + 8;
                // 首部4字节记录type
                outputStream.write(Constant.INT_TO_BYTES(1));
                // 再4字节记录k-v对的个数
                outputStream.write(Constant.INT_TO_BYTES(entrys.size()));
                // 依次写入各Entry
                for(Entry entry : entrys){
                    // 写key
                    outputStream.write(entry.key.serialize());
                    // 写value，其实是个记录offset的long
                    outputStream.write(Constant.LONG_TO_BYTES((long)entry.value));
                }
                return totalLength;
            }

            // 非叶子节点
            else{
                // 后根遍历
                for(BTreeNode child : children){
                    long childLength = child.write(outputStream, this.offset);
                    this.offset = child.offset + childLength;
                }
                // 将其子节点都保存完后再保存该节点
                // 每个Entry即每个k-v占用的字节数， 共有entrys.size()个
                int singleEntryLength = Constant.MAX_KEY_LENGTH + Long.BYTES;
                // 每个Children即一个记录偏移的龙占用的字节数，共有children.size()个
                int singleChildLength = Long.BYTES;
                // 额外还需要12字节，一个int记录节点type，一个int记录entry个数，一个int记录child个数
                int totalLength = singleEntryLength * entrys.size() + singleChildLength * children.size() + 12;
                // 首部4字节记录type
                outputStream.write(Constant.INT_TO_BYTES(0));
                // 再4字节记录k-v对的个数
                outputStream.write(Constant.INT_TO_BYTES(entrys.size()));
                // 依次写入各Entry
                for(Entry entry : entrys){
                    // 写key
                    outputStream.write(entry.key.serialize());
                    // 写value，其实是个记录offset的long
                    outputStream.write(Constant.LONG_TO_BYTES((long)entry.value));
                }
                // 再4字节记录children的个数
                outputStream.write(Constant.INT_TO_BYTES(children.size()));
                // 依次写入各Children
                for(BTreeNode child : children){
                    // 每个child保存其offset
                    outputStream.write(Constant.LONG_TO_BYTES((long)child.offset));
                }
                return totalLength;
            }
        }

    }

    private static final int DEFAULT_T = 2;

    /**
     * B树的根节点
     */
    private BTreeNode root;
    /**
     * 根据B树的定义，B树的每个非根节点的关键字数n满足(t - 1) <= n <= (2t - 1)
     */
    private int t = DEFAULT_T;
    /**
     * 非根节点中最小的键值数
     */
    private int minKeySize = t - 1;
    /**
     * 非根节点中最大的键值数
     */
    private int maxKeySize = 2 * t - 1;
    /**
     * 键的比较函数对象
     */
    private Comparator<K> kComparator;

    /**
     * 构造一颗B树，键值采用采用自然排序方式
     */
    public BTree() {
        root = new BTreeNode();
        root.setLeaf(true);
    }

    public BTree(int t) {
        this();
        this.t = t;
        minKeySize = t - 1;
        maxKeySize = 2 * t - 1;
    }

    // 构造函数，从文件fileName的offset偏移处读取root根节点，并解析构造B-Tree
    public BTree(RandomAccessFile raf, long offset){
        root = new BTreeNode(raf, offset);
    }

    /**
     * 以给定的键值比较函数对象构造一颗B树。
     *
     * @param kComparator - 键值的比较函数对象
     */
    public BTree(Comparator<K> kComparator) {
        root = new BTreeNode(kComparator);
        root.setLeaf(true);
        this.kComparator = kComparator;
    }

    public BTree(Comparator<K> kComparator, int t) {
        this(kComparator);
        this.t = t;
        minKeySize = t - 1;
        maxKeySize = 2 * t - 1;
    }

    @SuppressWarnings("unchecked")
    int compare(K key1, K key2) {
        return kComparator == null ? ((Comparable<K>) key1).compareTo(key2) : kComparator.compare(key1, key2);
    }

    /**
     * 搜索给定的键。
     *
     * @param key - 给定的键值
     * @return 键关联的值，如果存在，否则null
     */
    public long search(K key) {
        return search(root, key);
    }

    /**
     * 在以给定节点为根的子树中，递归搜索
     * 给定的<code>key</code>
     *
     * @param node - 子树的根节点
     * @param key  - 给定的键值
     * @return 键关联的值，如果存在，否则null
     */
    private long search(BTreeNode node, K key) {
        SearchResult<V> result = node.searchKey(key);
        if (result.isExist())
            return result.getValue();
        else {
            if (node.isLeaf())
                return 0;
            else
                return search(node.childAt(result.getIndex()), key);
        }
    }

    /**
     * 分裂一个满子节点<code>childNode</code>。
     * <p/>
     * 你需要自己保证给定的子节点是满节点。
     *
     * @param parentNode - 父节点
     * @param childNode  - 满子节点
     * @param index      - 满子节点在父节点中的索引
     */
    private void splitNode(BTreeNode parentNode, BTreeNode childNode, int index) {
        assert childNode.size() == maxKeySize;

        BTreeNode siblingNode = new BTreeNode(kComparator);
        siblingNode.setLeaf(childNode.isLeaf());
        // 将满子节点中索引为[t, 2t - 2]的(t - 1)个项插入新的节点中
        for (int i = 0; i < minKeySize; ++i)
            siblingNode.addEntry(childNode.entryAt(t + i));
        // 提取满子节点中的中间项，其索引为(t - 1)
        Entry entry = childNode.entryAt(t - 1);
        // 删除满子节点中索引为[t - 1, 2t - 2]的t个项
        for (int i = maxKeySize - 1; i >= t - 1; --i)
            childNode.removeEntry(i);
        if (!childNode.isLeaf()) // 如果满子节点不是叶节点，则还需要处理其子节点
        {
            // 将满子节点中索引为[t, 2t - 1]的t个子节点插入新的节点中
            for (int i = 0; i < minKeySize + 1; ++i)
                siblingNode.addChild(childNode.childAt(t + i));
            // 删除满子节点中索引为[t, 2t - 1]的t个子节点
            for (int i = maxKeySize; i >= t; --i)
                childNode.removeChild(i);
        }
        // 将entry插入父节点
        parentNode.insertEntry(entry, index);
        // 将新节点插入父节点
        parentNode.insertChild(siblingNode, index + 1);
    }

    /**
     * 在一个非满节点中插入给定的项。
     *
     * @param node  - 非满节点
     * @param entry - 给定的项
     * @return true，如果B树中不存在给定的项，否则false
     */
    private boolean insertNotFull(BTreeNode node, Entry entry) {
        assert node.size() < maxKeySize;

        if (node.isLeaf()) // 如果是叶子节点，直接插入
            return node.insertEntry(entry);
        else {
            /* 找到entry在给定节点应该插入的位置，那么entry应该插入
             * 该位置对应的子树中
             */
            SearchResult<V> result = node.searchKey(entry.getKey());
            // 如果存在，则直接返回失败
            if (result.isExist())
                return false;
            BTreeNode childNode = node.childAt(result.getIndex());
            if (childNode.size() == 2 * t - 1) // 如果子节点是满节点
            {
                // 则先分裂
                splitNode(node, childNode, result.getIndex());
                /* 如果给定entry的键大于分裂之后新生成项的键，则需要插入该新项的右边，
                 * 否则左边。
                 */
                if (compare(entry.getKey(), node.entryAt(result.getIndex()).getKey()) > 0)
                    childNode = node.childAt(result.getIndex() + 1);
            }
            return insertNotFull(childNode, entry);
        }
    }

    /**
     * 在B树中插入给定的键值对。
     *
     * @param key                       - 键
     * @param value                     - 值
     * @return true，如果B树中不存在给定的项，否则false
     */
    public boolean insert(K key, long value) {
        if (root.size() == maxKeySize) // 如果根节点满了，则B树长高
        {
            BTreeNode newRoot = new BTreeNode(kComparator);
            newRoot.setLeaf(false);
            newRoot.addChild(root);
            splitNode(newRoot, root, 0);
            root = newRoot;
        }
        return insertNotFull(root, new Entry(key, value));
    }

    /**
     * 如果存在给定的键，则更新键关联的值，
     * 否则插入给定的项。
     *
     * @param node  - 非满节点
     * @param entry - 给定的项
     * @return true，如果B树中不存在给定的项，否则false
     */
    private long putNotFull(BTreeNode node, Entry entry) {
        assert node.size() < maxKeySize;

        if (node.isLeaf()) // 如果是叶子节点，直接插入
            return node.putEntry(entry);
        else {
            /* 找到entry在给定节点应该插入的位置，那么entry应该插入
             * 该位置对应的子树中
             */
            SearchResult<V> result = node.searchKey(entry.getKey());
            // 如果存在，则更新
            if (result.isExist())
                return node.putEntry(entry);
            BTreeNode childNode = node.childAt(result.getIndex());
            if (childNode.size() == 2 * t - 1) // 如果子节点是满节点
            {
                // 则先分裂
                splitNode(node, childNode, result.getIndex());
                /* 如果给定entry的键大于分裂之后新生成项的键，则需要插入该新项的右边，
                 * 否则左边。
                 */
                if (compare(entry.getKey(), node.entryAt(result.getIndex()).getKey()) > 0)
                    childNode = node.childAt(result.getIndex() + 1);
            }
            return putNotFull(childNode, entry);
        }
    }

    /**
     * 如果B树中存在给定的键，则更新值。
     * 否则插入。
     *
     * @param key   - 键
     * @param value - 值
     * @return 如果B树中存在给定的键，则返回之前的值，否则null
     */
    public long put(K key, long value) {
        if (root.size() == maxKeySize) // 如果根节点满了，则B树长高
        {
            BTreeNode newRoot = new BTreeNode(kComparator);
            newRoot.setLeaf(false);
            newRoot.addChild(root);
            splitNode(newRoot, root, 0);
            root = newRoot;
        }
        return putNotFull(root, new Entry(key, value));
    }

    /**
     * 从B树中删除一个与给定键关联的项。
     *
     * @param key - 给定的键
     * @return 如果B树中存在给定键关联的项，则返回删除的项，否则null
     */
    public Entry delete(K key) {
        return delete(root, key);
    }

    /**
     * 从以给定<code>node</code>为根的子树中删除与给定键关联的项。
     * <p/>
     * 删除的实现思想请参考《算法导论》第二版的第18章。
     *
     * @param node - 给定的节点
     * @param key  - 给定的键
     * @return 如果B树中存在给定键关联的项，则返回删除的项，否则null
     */
    private Entry delete(BTreeNode node, K key) {
        // 该过程需要保证，对非根节点执行删除操作时，其关键字个数至少为t。
        assert node.size() >= t || node == root;

        SearchResult<V> result = node.searchKey(key);
        /*
         * 因为这是查找成功的情况，0 <= result.getIndex() <= (node.size() - 1)，
         * 因此(result.getIndex() + 1)不会溢出。
         */
        if (result.isExist()) {
            // 1.如果关键字在节点node中，并且是叶节点，则直接删除。
            if (node.isLeaf())
                return node.removeEntry(result.getIndex());
            else {
                // 2.a 如果节点node中前于key的子节点包含至少t个项
                BTreeNode leftChildNode = node.childAt(result.getIndex());
                if (leftChildNode.size() >= t) {
                    // 使用leftChildNode中的最后一个项代替node中需要删除的项
                    node.removeEntry(result.getIndex());
                    node.insertEntry(leftChildNode.entryAt(leftChildNode.size() - 1), result.getIndex());
                    // 递归删除左子节点中的最后一个项
                    return delete(leftChildNode, leftChildNode.entryAt(leftChildNode.size() - 1).getKey());
                } else {
                    // 2.b 如果节点node中后于key的子节点包含至少t个关键字
                    BTreeNode rightChildNode = node.childAt(result.getIndex() + 1);
                    if (rightChildNode.size() >= t) {
                        // 使用rightChildNode中的第一个项代替node中需要删除的项
                        node.removeEntry(result.getIndex());
                        node.insertEntry(rightChildNode.entryAt(0), result.getIndex());
                        // 递归删除右子节点中的第一个项
                        return delete(rightChildNode, rightChildNode.entryAt(0).getKey());
                    } else // 2.c 前于key和后于key的子节点都只包含t-1个项
                    {
                        Entry deletedEntry = node.removeEntry(result.getIndex());
                        node.removeChild(result.getIndex() + 1);
                        // 将node中与key关联的项和rightChildNode中的项合并进leftChildNode
                        leftChildNode.addEntry(deletedEntry);
                        for (int i = 0; i < rightChildNode.size(); ++i)
                            leftChildNode.addEntry(rightChildNode.entryAt(i));
                        // 将rightChildNode中的子节点合并进leftChildNode，如果有的话
                        if (!rightChildNode.isLeaf()) {
                            for (int i = 0; i <= rightChildNode.size(); ++i)
                                leftChildNode.addChild(rightChildNode.childAt(i));
                        }
                        return delete(leftChildNode, key);
                    }
                }
            }
        } else {
            /*
             * 因为这是查找失败的情况，0 <= result.getIndex() <= node.size()，
             * 因此(result.getIndex() + 1)会溢出。
             */
            if (node.isLeaf()) // 如果关键字不在节点node中，并且是叶节点，则什么都不做，因为该关键字不在该B树中
            {
                //logger.info("The key: " + key + " isn't in this BTree.");
                return null;
            }
            BTreeNode childNode = node.childAt(result.getIndex());
            if (childNode.size() >= t) // // 如果子节点有不少于t个项，则递归删除
                return delete(childNode, key);
            else // 3
            {
                // 先查找右边的兄弟节点
                BTreeNode siblingNode = null;
                int siblingIndex = -1;
                if (result.getIndex() < node.size()) // 存在右兄弟节点
                {
                    if (node.childAt(result.getIndex() + 1).size() >= t) {
                        siblingNode = node.childAt(result.getIndex() + 1);
                        siblingIndex = result.getIndex() + 1;
                    }
                }
                // 如果右边的兄弟节点不符合条件，则试试左边的兄弟节点
                if (siblingNode == null) {
                    if (result.getIndex() > 0) // 存在左兄弟节点
                    {
                        if (node.childAt(result.getIndex() - 1).size() >= t) {
                            siblingNode = node.childAt(result.getIndex() - 1);
                            siblingIndex = result.getIndex() - 1;
                        }
                    }
                }
                // 3.a 有一个相邻兄弟节点至少包含t个项
                if (siblingNode != null) {
                    if (siblingIndex < result.getIndex()) // 左兄弟节点满足条件
                    {
                        childNode.insertEntry(node.entryAt(siblingIndex), 0);
                        node.removeEntry(siblingIndex);
                        node.insertEntry(siblingNode.entryAt(siblingNode.size() - 1), siblingIndex);
                        siblingNode.removeEntry(siblingNode.size() - 1);
                        // 将左兄弟节点的最后一个孩子移到childNode
                        if (!siblingNode.isLeaf()) {
                            childNode.insertChild(siblingNode.childAt(siblingNode.size()), 0);
                            siblingNode.removeChild(siblingNode.size());
                        }
                    } else // 右兄弟节点满足条件
                    {
                        childNode.insertEntry(node.entryAt(result.getIndex()), childNode.size() - 1);
                        node.removeEntry(result.getIndex());
                        node.insertEntry(siblingNode.entryAt(0), result.getIndex());
                        siblingNode.removeEntry(0);
                        // 将右兄弟节点的第一个孩子移到childNode
                        // childNode.insertChild(siblingNode.childAt(0), childNode.size() + 1);
                        if (!siblingNode.isLeaf()) {
                            childNode.addChild(siblingNode.childAt(0));
                            siblingNode.removeChild(0);
                        }
                    }
                    return delete(childNode, key);
                } else // 3.b 如果其相邻左右节点都包含t-1个项
                {
                    if (result.getIndex() < node.size()) // 存在右兄弟，直接在后面追加
                    {
                        BTreeNode rightSiblingNode = node.childAt(result.getIndex() + 1);
                        childNode.addEntry(node.entryAt(result.getIndex()));
                        node.removeEntry(result.getIndex());
                        node.removeChild(result.getIndex() + 1);
                        for (int i = 0; i < rightSiblingNode.size(); ++i)
                            childNode.addEntry(rightSiblingNode.entryAt(i));
                        if (!rightSiblingNode.isLeaf()) {
                            for (int i = 0; i <= rightSiblingNode.size(); ++i)
                                childNode.addChild(rightSiblingNode.childAt(i));
                        }
                    } else // 存在左节点，在前面插入
                    {
                        BTreeNode leftSiblingNode = node.childAt(result.getIndex() - 1);
                        childNode.insertEntry(node.entryAt(result.getIndex() - 1), 0);
                        node.removeEntry(result.getIndex() - 1);
                        node.removeChild(result.getIndex() - 1);
                        for (int i = leftSiblingNode.size() - 1; i >= 0; --i)
                            childNode.insertEntry(leftSiblingNode.entryAt(i), 0);
                        if (!leftSiblingNode.isLeaf()) {
                            for (int i = leftSiblingNode.size(); i >= 0; --i)
                                childNode.insertChild(leftSiblingNode.childAt(i), 0);
                        }
                    }
                    // 如果node是root并且node不包含任何项了
                    if (node == root && node.size() == 0)
                        root = childNode;
                    return delete(childNode, key);
                }
            }
        }
    }

    /**
     * 一个简单的层次遍历B树实现，用于输出B树。
     */
    public void output() {
        Queue<BTreeNode> queue = new LinkedList<BTreeNode>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            BTreeNode node = queue.poll();
            for (int i = 0; i < node.size(); ++i)
                System.out.print(node.entryAt(i) + " ");
            System.out.println();
            if (!node.isLeaf()) {
                for (int i = 0; i <= node.size(); ++i)
                    queue.offer(node.childAt(i));
            }
        }
    }


    // BTNodes的持久化保存, 写到fileName的开始偏移为offset的地方，返回值 1.存储占用的总字节数 2.根节点所在偏移
    public long[] write(BufferedOutputStream outputStream, long offset){
        try{
            root.write(outputStream, offset);
        }catch (Exception e){
            e.printStackTrace();
        }

        // 计算占用总字节数
        long totalSize;
        if(root.leaf){
            totalSize = Integer.BYTES * 2 + root.entrys.size() * (Constant.MAX_KEY_LENGTH + Long.BYTES);
        }
        else{
            totalSize = Integer.BYTES * 3 +
                    root.entrys.size() * (Constant.MAX_KEY_LENGTH + Long.BYTES) +
                    root.children.size() * Long.BYTES;
        }

        long[] ret = new long[2];
        ret[0] = root.offset + totalSize - offset;
        ret[1] = root.offset;
        return ret;
    }

    // 查询比key小的最大key对应的value
    public long leftSearch(K key){
        // 如果key比最大key还大，直接返回null
        if(compare(key, getMaxKey()) > 0)
            return 0;
        return root.leftSearch(key, null);
    }

    // 返回B树的最大key
    public K getMaxKey() {
        if(this.root == null || this.root.entrys.size() == 0)
            return null;
        BTreeNode cur = this.root;
        while(cur.children != null && cur.children.size() > 0){
            cur = (BTreeNode) cur.children.get(cur.children.size() - 1);
        }
        return (K) ((Entry)cur.entrys.get(cur.entrys.size() - 1)).getKey();
    }
}