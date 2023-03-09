package drz.tmdb;

import org.junit.Test;

import java.io.IOException;

import drz.tmdb.Level.SSTable;
import drz.tmdb.Memory.MemManager;
import drz.tmdb.Memory.SystemTable.ObjectTableItem;

public class StorageTest {
    @Test
    public void writeSSTable() throws IOException,InterruptedException {
        // SSTable读写测试
        MemManager memManager = new MemManager();
        for(int i=1; i<50000; i++){
            memManager.add(new ObjectTableItem());
        }
        // 写
        long t1 = System.currentTimeMillis();
        memManager.saveMemTableToFile();
        long t2 = System.currentTimeMillis();
        // 读
        SSTable f = new SSTable("SSTable1", 2);
        long t3 = System.currentTimeMillis();
        System.out.println("50000个键值对写入SSTable，耗时" + (t2 - t1) + "ms");
        System.out.println("读取SSTable的meta data，耗时" + (t3 - t2) + "ms");
    }
}
