package drz.oddb.sync.statistics;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class WriteCSV {

    public static String filePath = System.getProperty("user.dir")+"/src/main/java/drz/oddb/sync/statistics/data";

    public static void writeCSVFile(String fileName, ArrayList<String[]> data ) {
        CSVWriter writer = null;

        File file = new File(filePath);
        if (!file.exists()){
            file.mkdirs();
        }

        String absolutePath = filePath + "/" + fileName + ".csv";

        try {
            // 创建文件所在目录
            FileOutputStream fileOutputStream = new FileOutputStream(absolutePath);
            fileOutputStream.write(0xef); //加上这句话
            fileOutputStream.write(0xbb); //加上这句话
            fileOutputStream.write(0xbf); //加上这句话

            writer = new CSVWriter(
                    new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8.name()),
                    CSVWriter.DEFAULT_SEPARATOR,
                    CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);

            writer.writeAll(data);
        } catch (Exception e) {
            System.out.println("将数据写入CSV出错："+e);
        } finally {
            if (null != writer) {
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    System.out.println("关闭文件输出流出错："+e);
                }
            }
        }

    }


    public static <T> ArrayList<String[]> getData(T t){
        ArrayList<String[]> result = new ArrayList<>();

        Field[] fields = t.getClass().getDeclaredFields();
        int len = fields.length;
        String[] attributes = new String[len];
        String[] values = new String[len];


        for (int i = 0; i < len; i++){
            fields[i].setAccessible(true);//将可能为private的成员设为可读
            try {
                attributes[i] = fields[i].getName();
                values[i] = fields[i].get(t).toString();
            }catch (IllegalAccessException e){
                e.printStackTrace();
            }
        }

        result.add(attributes);
        result.add(values);

        return result;
    }
}
