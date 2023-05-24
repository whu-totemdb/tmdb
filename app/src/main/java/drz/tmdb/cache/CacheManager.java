package drz.tmdb.cache;


// 缓存由两部分组成
// 一部分记录若干SSTable的meta data部分，用于加速SSTable的读取过程
// 一部分记录hot key，根据LRU原则进行替换
public class CacheManager {

    public DataCache dataCache = new DataCache();
    public MetaCache metaCache = new MetaCache();


}
