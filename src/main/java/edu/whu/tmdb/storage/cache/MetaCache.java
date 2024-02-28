package edu.whu.tmdb.storage.cache;


import edu.whu.tmdb.storage.level.SSTable;
import edu.whu.tmdb.storage.utils.Constant;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;


// 缓存SSTable的meta block
public class MetaCache {

    public Map<Integer, SSTable> metas = new HashMap<>();

    public void add(SSTable newSST){
        int fileSuffix = Integer.parseInt(newSST.fileName.split("SSTable")[1]);
        this.metas.put(fileSuffix, newSST);

        // 加入缓存的SSTable多用于读，因此读通道打开
        try{
            if(newSST.raf == null)
                newSST.raf = new RandomAccessFile(new File(Constant.DATABASE_DIR + newSST.fileName), "r");
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void remove(int fileSuffix){
        metas.remove(fileSuffix);
    }

    // 根据fileSuffix返回对应SSTable的meta block
    public SSTable get(int fileSuffix){
        SSTable sst = metas.getOrDefault(fileSuffix, null);

        // 如果缓存中没有，则从磁盘上加载
        if(sst == null){
            sst = new SSTable("SSTable" + fileSuffix, 3);
            metas.put(fileSuffix, sst);
        }
        return sst;
    }
}
