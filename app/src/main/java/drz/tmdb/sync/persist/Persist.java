package drz.tmdb.sync.persist;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class Persist {

    private MappedByteBuffer mappedByteBuffer;

    public static int length = 0x080000;//0x8000000--128M

    public Persist() {}

    public Persist(String pathName,String fileName) {
        try {
            mappedByteBuffer = map(pathName,fileName);
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    public static MappedByteBuffer map(String pathName,String fileName) throws IOException {
        File file = new File(pathName,fileName);

        if (!file.exists()){
            file.getParentFile().mkdirs();
            file.createNewFile();
        }

        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();

        MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, length);

        return buffer;
    }

    //从当前mmap指针开始写入数据data
    public void write(byte[] data){
        mappedByteBuffer.put(data);
        if (!mappedByteBuffer.isLoaded()) {
            mappedByteBuffer.load();
        }

    }

    //从指定的pos开始写入数据data
    public void write(byte[] data,int pos){

        MappedByteBuffer subBuffer = (MappedByteBuffer) mappedByteBuffer.slice();
        subBuffer.position(pos);
        subBuffer.put(data);
        if (!mappedByteBuffer.isLoaded()){
            mappedByteBuffer = subBuffer.load();
        }
        else {
            mappedByteBuffer = subBuffer;
        }

    }

    //从当前mmap指针开始读取size个比特数据
    public byte[] read(int size){
        byte[] data = new byte[size];
        int i = 0;

        MappedByteBuffer tmpBuffer = (MappedByteBuffer) mappedByteBuffer.flip();//切换至读模式

        while (tmpBuffer.remaining() > 0) {
            byte b = tmpBuffer.get();
            data[i] = b;
            i++;
        }

        mappedByteBuffer.limit(mappedByteBuffer.capacity());

        return data;
    }

    //从指定的pos开始读取size个比特数据
    public byte[] read(int size,int pos){
        byte[] data = new byte[size];
        MappedByteBuffer tmpBuffer = (MappedByteBuffer) mappedByteBuffer.flip();//切换至读模式

        tmpBuffer.position(pos);
        tmpBuffer.get(data);

        mappedByteBuffer.limit(mappedByteBuffer.capacity());

        return data;
    }

    //标记调用时position的值
    public MappedByteBuffer mark(){
        return (MappedByteBuffer) mappedByteBuffer.mark();
    }
}
