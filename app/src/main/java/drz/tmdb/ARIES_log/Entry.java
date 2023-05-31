package drz.tmdb.ARIES_log;

import drz.tmdb.cache.K;
import drz.tmdb.cache.V;

public class Entry {
    public EntryId Entry_id;
    public K key;
    public V value;
    public Boolean dirty;
    public Entry(K key,V value,Boolean dirty){
        this.key=key;
        this.value=value;
        this.dirty=dirty;
    }

    public Entry(){
    }
    EntryId getId(){
        return Entry_id;
    }


}
