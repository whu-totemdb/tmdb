package drz.tmdb.cache;

import java.util.HashMap;
import java.util.Map;

import drz.tmdb.level.SSTable;

// 缓存level 0 - level 2 的SSTable的meta block
public class MetaCache {

    private Map<Integer, SSTable> metas = new HashMap<>();

    public void add(SSTable newSST){
        int fileSuffix = Integer.parseInt(newSST.fileName.split("SSTable")[1]);
        this.metas.put(fileSuffix, newSST);
    }

    public void remove(int fileSuffix){
        metas.remove(fileSuffix);
    }

    // 根据fileSuffix返回对应SSTable的meta block
    public SSTable get(int fileSuffix){
        return metas.getOrDefault(fileSuffix, null);
    }
}
