//package edu.whu.tmdb.Transaction.Transactions.torch;
//
//import android.content.Context;
//import android.database.Cursor;
//import android.database.sqlite.SQLiteDatabase;
//import android.database.sqlite.SQLiteOpenHelper;
//
//import bdm.Torch.base.db.AndroidSQLiteHelper;
//
//
//public class TorchSQLiteHelper extends SQLiteOpenHelper implements AndroidSQLiteHelper {
//
////    private static String DATABASE_NAME="";
//    private static final int DATABASE_VERSION = 1;
//
//
//    public TorchSQLiteHelper(Context context, String DATABASE_NAME) {
//        super(context, DATABASE_NAME, null, DATABASE_VERSION);
//    }
//
//    @Override
//    public void onCreate(SQLiteDatabase sqLiteDatabase) {
//
//    }
//
//    @Override
//    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
//
//    }
//
//    public void query(){
//        SQLiteDatabase db = getWritableDatabase();
//
//
//    }
//
//    public Cursor execSql(String s){
//        return getWritableDatabase().rawQuery(s, new String[]{});
//    }
//
//    @Override
//    public void execSQL(String s) {
//        getWritableDatabase().rawQuery(s, new String[]{});
//    }
//
//    @Override
//    public void execSQL(String s, String[] strings) {
//        getWritableDatabase().rawQuery(s,strings);
//    }
//
//    @Override
//    public String query(String s, String s1, String[] strings) {
//        Cursor query = getReadableDatabase().query(s, null, s1, strings, null, null, null);
//
//        if (query.moveToFirst()) {
//            String string = query.getString(1);// 获取第一列的值
//            query.close();
//            return string;
//// 在这里处理获取到的值
//        } else {
//            return null;// 查询结果为空
//        }
//    }
//
//
//
//
//}
//
//
//
//
