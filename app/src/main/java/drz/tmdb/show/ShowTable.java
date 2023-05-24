package drz.tmdb.show;

import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.ListView;

import androidx.appcompat.app.AppCompatActivity;

import java.io.Serializable;

import drz.tmdb.R;
import drz.tmdb.memory.SystemTable.BiPointerTable;
import drz.tmdb.memory.SystemTable.ClassTable;
import drz.tmdb.memory.SystemTable.DeputyTable;
import drz.tmdb.memory.SystemTable.ObjectTable;
import drz.tmdb.memory.SystemTable.SwitchingTable;

public class ShowTable extends AppCompatActivity implements Serializable {
    private String[] tables = new String[5];
    Context context;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        showTable();
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_show_class);

        Intent intent0 = getIntent();
        Bundle bundle0 = intent0.getExtras();
        final ObjectTable topt = (ObjectTable) bundle0.getSerializable("ObjectTable");
        final SwitchingTable switchingT = (SwitchingTable)bundle0.getSerializable("SwitchingTable");
        final DeputyTable deputyt = (DeputyTable)bundle0.getSerializable("DeputyTable");
        final BiPointerTable biPointerT = (BiPointerTable)bundle0.getSerializable("BiPointerTable");
        final ClassTable classTable = (ClassTable)bundle0.getSerializable("ClassTable");

        ArrayAdapter<String> adapter = new ArrayAdapter<String>(ShowTable.this,android.R.layout.simple_list_item_1,tables);
        ListView tableList = (ListView)findViewById(R.id.tablelist);
        tableList.setAdapter(adapter);
        tableList.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> adapterView, View view, int i, long l) {
                //String curClass = tables[i];
                //Intent intent = new Intent(ShowTable.this,ShowObj.class);
                //startActivity(intent);
                switch (i){
                    case 0:
                    {
                        printObj(topt);
                        break;
                    }
                    case 1:{
                        printSwi(switchingT);
                        break;
                    }
                    case 2:{
                        printDep(deputyt);
                        break;
                    }
                    case 3:{
                       printBi(biPointerT);
                        break;
                    }
                    case 4:{
                       printCla(classTable);
                        break;
                    }
                }
            }
        });

    }
    private void showTable(){
        tables[0]="ObjectTable";
        tables[1]="SwitchingTable";
        tables[2]="DeputyTable";
        tables[3]="BiPointerTable";
        tables[4]="ClassTable";
    }

    private void printObj(ObjectTable topt){
        Intent intent = new Intent(ShowTable.this, ShowObj.class);

        Bundle bundle0 = new Bundle();
        bundle0.putSerializable("ObjectTable",topt);
        intent.putExtras(bundle0);
        startActivity(intent);
    }

    private void printSwi(SwitchingTable switchingT){
        Intent intent = new Intent(ShowTable.this, ShowSwi.class);

        Bundle bundle0 = new Bundle();
        bundle0.putSerializable("SwitchingTable",switchingT);
        intent.putExtras(bundle0);
        startActivity(intent);
    }

    private void printDep(DeputyTable deputyt){
        Intent intent = new Intent(ShowTable.this, ShowDep.class);

        Bundle bundle0 = new Bundle();
        bundle0.putSerializable("DeputyTable",deputyt);
        intent.putExtras(bundle0);
        startActivity(intent);
    }

    private void printBi(BiPointerTable biPointerT){
        Intent intent = new Intent(ShowTable.this, ShowBi.class);

        Bundle bundle0 = new Bundle();
        bundle0.putSerializable("BiPointerTable",biPointerT);
        intent.putExtras(bundle0);
        startActivity(intent);
    }

    private void printCla(ClassTable classTable){
        Intent intent = new Intent(ShowTable.this, ShowCla.class);

        Bundle bundle0 = new Bundle();
        bundle0.putSerializable("ClassTable",classTable);
        intent.putExtras(bundle0);
        startActivity(intent);
    }

}
