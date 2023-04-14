package drz.tmdb;

import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import androidx.appcompat.app.AlertDialog;
import androidx.appcompat.app.AppCompatActivity;


import java.io.IOException;

import drz.tmdb.Transaction.TransAction;
import drz.tmdb.map.MapActivity;

public class MainActivity extends AppCompatActivity {

    //查询输入框

    private EditText editText;

    private TextView text_view;
    TransAction trans = new TransAction(this);
    //Node node;

    private boolean whu_trace_select = false;




    public MainActivity() throws IOException {
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);


        /*new Thread(()->{

            try {
                //System.out.println(FilePathUtil.getFileDir(this));
                //Sync.setPathName(FilePathUtil.getFileDir(this));
                Sync.initialNode(9090,this);
                //node.start();
            }catch (Exception e){
                e.printStackTrace();
            }

        },"initialNodeThread").start();*/

        //查询按钮
        Button button = findViewById(R.id.button);
        editText = findViewById(R.id.edit_text);
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
//                onStop();
//                trans.Test();
//                try {
//                    trans.clear();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
                trans.query2("",-1,editText.getText().toString());
           }
        });

        //退出按钮
        Button exit_button = findViewById(R.id.exit_button);
        exit_button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                showexitdialog(v);
            }
        });

        //展示按钮
        Button show_button = findViewById(R.id.showbutton);
        show_button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                //onStop();
                trans.PrintTab();
            }
        });

        // 清除文本框内数据
        Button clean_button = findViewById(R.id.clean_button);
        clean_button.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v){
                //editText = findViewById(R.id.edit_text);
                editText.setText("");
            }
        });


        // 绘制地图
        Button draw_trace = findViewById(R.id.draw_trace);
        draw_trace.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v) {
                startActivity(new Intent(MainActivity.this, MapActivity.class));
            }
        });


        Button whu_trace = findViewById(R.id.whu);
        whu_trace.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v){
                if(whu_trace_select){
                    whu_trace_select = false;
                }
                else{
                    whu_trace_select = true;
                }
            }
        });



       /* //广播按钮
        Button broadcast_button = findViewById(R.id.broadcast_button);

        broadcast_button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                new Thread(() -> {
                    try {
                        Sync.broadcast();
                    }catch (UnknownHostException e){
                        e.printStackTrace();
                    }
                },"broadcastThread").start();


            }
        });*/



        //同步按钮
        /*Button sync_button = findViewById(R.id.sync_button);
        sync_button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {

                //insert into test (name,age,number) values("a",1,10.0);
                Action action = new Action(
                    OperationType.insert,
                    "test",
                    "test",
                    1,
                    3,
                    new String[]{"name", "age", "number"},
                    new String[]{"String", "Integer", "Double"},
                    new String[]{"a", "1", "10.0"});

                Sync.syncStart(action);

            }
        });*/



    }


    protected void onStop(){
        super.onStop();
        Log.e("main", "...onstop");
    }

    protected void onStart(){
        super.onStart();
        Log.e("main","...onstart");
    }

    //点击exit_button退出程序
    public void showexitdialog(View v){
        //定义一个新对话框对象
        AlertDialog.Builder exit_dialog = new AlertDialog.Builder(this);
        //设置对话框提示内容
        exit_dialog.setMessage("Do you want to save it before exiting the program?");
        //定义对话框两个按钮及接受事件
        exit_dialog.setPositiveButton("YES", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                //保存
                try {
                    trans.SaveAll();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //退出
                android.os.Process.killProcess(android.os.Process.myPid());

            }
        });
        exit_dialog.setNegativeButton("NO", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                //退出
                android.os.Process.killProcess(android.os.Process.myPid());

            }
        });
        //创建并显示对话框
        AlertDialog exit_dialog0 = exit_dialog.create();
        exit_dialog0.show();

    }

}
