package drz.tmdb.sync.statistics;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class WriteCSV {

    public static String filePath = System.getProperty("user.dir")+"/src/main/java/drz/tmdb/sync/statistics/data";


    //参数append表示是否是向已经存在的csv文件追加内容
    public static void writeCSVFile(String fileName, ArrayList<String[]> header, ArrayList<String[]> data, boolean append) {
        CSVWriter writer = null;

        File file = new File(filePath);
        if (!file.exists()){
            file.mkdirs();
        }

        String absolutePath = filePath + "/" + fileName + ".csv";

        try {
            // 创建文件所在目录
            FileOutputStream fileOutputStream = new FileOutputStream(absolutePath,append);

            writer = new CSVWriter(
                    new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8.name()),
                    CSVWriter.DEFAULT_SEPARATOR,
                    CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);

            if(!append){
                writer.writeAll(header);
            }
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

    public static void writeCSVFile(String fileName, ArrayList<String[]> data, boolean append) {
        CSVWriter writer = null;

        File file = new File(filePath);
        if (!file.exists()){
            file.mkdirs();
        }

        String absolutePath = filePath + "/" + fileName + ".csv";

        try {
            // 创建文件所在目录
            FileOutputStream fileOutputStream = new FileOutputStream(absolutePath,append);

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


    //获取类T的属性名
    public static <T> ArrayList<String[]> getAttributes(T t){
        ArrayList<String[]> result = new ArrayList<>();

        Field[] fields = t.getClass().getDeclaredFields();
        int len = fields.length;
        String[] attributes = new String[len];

        for (int i = 0; i < len; i++){
            fields[i].setAccessible(true);//将可能为private的成员设为可读
            attributes[i] = fields[i].getName();
        }

        result.add(attributes);

        return result;
    }

    //获取类T的对象t各个属性的值
    public static <T> String[] getAttributeValue(T t){

        Field[] fields = t.getClass().getDeclaredFields();
        int len = fields.length;
        //String[] attributes = new String[len];
        String[] values = new String[len];


        for (int i = 0; i < len; i++){
            fields[i].setAccessible(true);//将可能为private的成员设为可读
            try {
                //attributes[i] = fields[i].getName();
                values[i] = fields[i].get(t).toString();
            }catch (IllegalAccessException e){
                e.printStackTrace();
            }
        }

        //result.add(attributes);

        return values;
    }
}
