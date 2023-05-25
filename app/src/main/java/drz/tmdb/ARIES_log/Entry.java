package drz.tmdb.ARIES_log;

public class Entry {
    public EntryId Entry_id;
    public String key;
    public String value;
    public Boolean dirty;
    public Entry(String key,String value,Boolean dirty){
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
