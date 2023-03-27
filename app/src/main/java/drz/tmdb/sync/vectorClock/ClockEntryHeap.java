package drz.tmdb.sync.vectorClock;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClockEntryHeap {
    private final int maxNum;

    private ClockEntry[] heap;

    private int count;

    private int entryNumLimit;

    public HashMap<String,Integer> indexes;


    public ClockEntryHeap(int maxNum, int entryNumLimit) {
        this.maxNum = maxNum;
        this.heap = new ClockEntry[maxNum];
        this.count = 0;
        this.entryNumLimit = entryNumLimit;

        this.indexes = new HashMap<>();

    }

    public void put(String nodeID){
        ClockEntry entry;
        if (indexes.get(nodeID)==null){
            entry = new ClockEntry(nodeID);
        }
        else {
            entry = getClockEntry(nodeID);
            entry.lastUpdateTime = System.currentTimeMillis();
            remove(indexes.get(nodeID));
        }

        if (count == 0){
            heap[0] = entry;
            indexes.put(nodeID,0);
        }
        else {
            siftUp(entry,count);
        }

        count++;
    }

    private void siftUp(ClockEntry entry,int index){
        int parentIndex = 0;

        do {
            parentIndex = (index-1)/2;//父节点的索引
            if (heap[parentIndex].lastUpdateTime>entry.lastUpdateTime){
                indexes.put(heap[parentIndex].nodeID,index);
                heap[index] = heap[parentIndex];
                index = parentIndex;
            }
            else {
                break;
            }
        }while (parentIndex > 0);

        heap[index] = entry;
        indexes.put(entry.nodeID,index);
    }

    private void siftDown(ClockEntry entry,int index,int count){
        int end = count / 2;

        while (index < end){
            int childIndex = index*2+1;//左孩子节点索引

            ClockEntry clockEntry = heap[childIndex];
            if ((childIndex+1<count) && (clockEntry.lastUpdateTime > heap[childIndex+1].lastUpdateTime)){
                childIndex = childIndex + 1;
                clockEntry = heap[childIndex];
            }

            if (entry.lastUpdateTime > clockEntry.lastUpdateTime){
                indexes.put(heap[childIndex].nodeID,index);
                heap[index] = heap[childIndex];
                index = childIndex;
            }
            else {
                break;
            }
        }

        heap[index] = entry;
        indexes.put(entry.nodeID,index);
    }

    private int getNodeNum(int index){
        //本节点不存在
        if (index >= count){
            return 0;
        }

        //节点存在但没有右孩子
        if (2*index + 2 >= count){
            //如果也没有左孩子
            if (2*index+1>=count){
                return 1;
            }
            else {
                //存在左孩子
                return 2;
            }
        }

        //节点存在且有右孩子，那么肯定也有左孩子
        int leftNum = getNodeNum(2*index+1);
        int rightNum = getNodeNum(2*index+2);

        return (leftNum+rightNum+1);
    }


    private int getSubHeapLastIndex(int c,int index){
        ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<>();

        queue.add(index);

        int front;

        while (!queue.isEmpty()){
            front = queue.poll();

            if (2*front+1 < count){
                queue.add(2*front+1);
            }
            if (2*front+2 < count){
                queue.add(2*front+2);
            }
            c--;
            if (c <= 0){
                return front;
            }
        }

        return -1;


    }

    public void remove(int index){
        if (count == 0 || index >= count){
            return;
        }

        ClockEntry topEntry = heap[index];//以heap[index]为顶点的子堆的堆顶元素
        int num = getNodeNum(index);//获取以该节点为堆顶的子堆所具有的节点数量

        int i = getSubHeapLastIndex(num,index);

        if (i == -1){
            return;
        }

        ClockEntry clockEntry = heap[i];//子堆的最后一个元素
        indexes.remove(topEntry.nodeID);

        heap[i] = null;
        count--;

        siftDown(clockEntry,index,i);//从堆顶开始下沉
    }

    public ClockEntry peek(){
        if (count > 0){
            return heap[0];
        }
        else {
            return null;
        }
    }

    public ClockEntry getClockEntry(String nodeID){
        int index = indexes.get(nodeID);

        return heap[index];
    }
}
