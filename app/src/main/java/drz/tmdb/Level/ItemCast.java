package drz.tmdb.Level;

import static drz.tmdb.Level.Constant.DATABASE_DIR;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

// 在进行compaction时，为了避免将数据从SSTable解析到内存占用大量时间,创建类ItemCast
// SSTable中的某一项在内存中的投影，记录索引表中的相关信息
public class ItemCast {

    public String key;
    public int dataFileSuffix;
    public int offset;
    public int length;

    public ItemCast(int dataFileSuffix, int offset, int length) {
        this.dataFileSuffix = dataFileSuffix;
        this.offset = offset;
        this.length = length;
    }

    public byte[] getBytes(){
        byte[] buff = new byte[this.length];
        try{
            FileInputStream input = new FileInputStream(new File(DATABASE_DIR + "data" + this.dataFileSuffix));
            //指定偏移量开始读文件
            input.skip(this.offset);
            input.read(buff, 0, this.length);
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e) {
            e.printStackTrace();
        }
        return buff;
    }
}
