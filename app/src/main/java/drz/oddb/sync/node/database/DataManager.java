package drz.oddb.sync.node.database;

public class DataManager {

    private final int size;

    private byte[] data;


    public DataManager(int size) {
        this.size = size;
        data = new byte[size];
    }


    public void setData(byte[] data) {
        if(data.length > size) {
            throw new RuntimeException("传输的数据太大，无法全部储存");
        }

        this.data = data;
    }


}
