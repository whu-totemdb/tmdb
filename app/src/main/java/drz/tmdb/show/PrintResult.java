package drz.tmdb.show;

import android.content.Intent;
import android.os.Bundle;
import android.view.Gravity;
import android.view.ViewGroup;
import android.widget.TableLayout;
import android.widget.TableRow;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;

import drz.tmdb.memory.TupleList;
import drz.tmdb.R;

public class PrintResult extends AppCompatActivity {

    private final int W = ViewGroup.LayoutParams.WRAP_CONTENT;
    private final int M = ViewGroup.LayoutParams.MATCH_PARENT;
    private TableLayout rst_tab;

    @Override
    protected void onCreate(Bundle savedInstanceState){
        super.onCreate(savedInstanceState);
        setContentView(R.layout.print_result);
        Intent intent = getIntent();
        Bundle bundle = intent.getExtras();
//        Print((TupleList) bundle.getSerializable("tupleList"),bundle.getStringArray("attrname"),bundle.getIntArray("attrid"),bundle.getStringArray("type"),bundle.getString("removeDuplicate"));
        Print((TupleList) bundle.getSerializable("tupleList"),bundle.getStringArray("attrname"),bundle.getIntArray("attrid"),bundle.getStringArray("type"),"false");
    }

     public void Print(TupleList tpl,String[] attrname,int[] attrid,String[] type,String removeDuplicate){

        int tabCol  = attrid.length;
        int tabH = tpl.tuplelist.size();
        int r;
        int c;
        String stemp;
        int itemp;
        Object oj;

        rst_tab = findViewById(R.id.rst_tab);
        System.out.println(removeDuplicate);
       
        if (removeDuplicate.equals("true")) {
            String [][]record_list = new String [tabH][tabCol];
            System.out.println("removeDuplicate");
            for (int i = 0; i < tabCol; i++) {
                record_list[0][i] = (tpl.tuplelist.get(0).tuple[attrid[i]]).toString();
            }
            int index = 1; // 去重之后的数组长度

            for(int i=1;i<tabH;i++){
                String []temp_item = new String[tabCol];
                for(int j=0;j<tabCol;j++){
                    temp_item[j] = (tpl.tuplelist.get(i).tuple[attrid[j]]).toString();
                }
                if(!isDuplicate(record_list,temp_item,index)){
                    for(int j=0;j<tabCol;j++){
                        record_list[index][j] = temp_item[j];
                    }
                    index++;
                }
            }
            for (r=0;r<=index;r++){
                TableRow tableRow = new TableRow(this);
                for (c=0;c<tabCol;c++){
                    TextView tv = new TextView(this);
                    if (r==0)tv.setText(attrname[c]);
                    else tv.setText(record_list[r-1][c]);

                    tv.setGravity(Gravity.CENTER);
                    tv.setBackgroundResource(R.drawable.tab_bg);
                    tv.setTextSize(25);
                    tableRow.addView(tv);
                }
                rst_tab.addView(tableRow,new TableLayout.LayoutParams(M,W));
            }
        }

        else{
            for(r = 0;r <= tabH;r++){
                TableRow tableRow = new TableRow(this);
                for(c = 0;c < tabCol;c++){
                    TextView tv = new TextView(this);
                    if(r == 0){
                        tv.setText(attrname[c]);
                    }
                    else{
                        oj = tpl.tuplelist.get(r-1).tuple[attrid[c]];
                        // System.out.println(oj.getClass());
                        // System.out.println(type[attrid[c]]);
                        // System.out.println(oj.toString());
                        // System.out.println(Integer.parseInt(oj.toString()));
                        switch (type[c]){
                            case "int":
                                //itemp = Integer.parseInt(oj.toString());
                                if(oj==null) oj="null";
                                tv.setText(oj.toString()+"");
                                break;
                            case "char":
                                if(oj==null) oj="null";
                                stemp = oj.toString();
                                tv.setText(stemp);
                                break;
                        }
                    }
                    tv.setGravity(Gravity.CENTER);
                    tv.setBackgroundResource(R.drawable.tab_bg);
                    tv.setTextSize(25);
    
                    tableRow.addView(tv);
                }
                rst_tab.addView(tableRow,new TableLayout.LayoutParams(M,W));
            }
        }
    }

    public boolean isDuplicate(String[][] record_list,String[] temp_item,int index){
        int tabH = record_list.length;
        int tabCol = record_list[0].length;
        for(int i=0;i<index;i++){
            for(int j=0;j<tabCol;j++){
                if(record_list[i][j].equals(temp_item[j])){
                    if (j == tabCol-1){
                        return true;
                    }
                }
                else{
                    break;
                }
            }
        }
        return false;
    }

}
