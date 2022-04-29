package drz.oddb;
import android.app.Service;
import android.content.Intent;
import android.media.MediaPlayer;
import android.os.IBinder;

public class MusicServer extends Service{
    private MediaPlayer mediaPlayer;

    @Override
    public IBinder onBind(Intent intent){
        //TODO Auto-generated method stub
        return null;
    }

    @Override
    public void onStart(Intent intent,int startld){
        super.onStart(intent,startld);
        if(mediaPlayer==null){
            //R.raw.xxx 是资源文件
            mediaPlayer = MediaPlayer.create(this,R.raw.old_memory);
            mediaPlayer.setLooping(true);
            mediaPlayer.start();
        }
    }

    @Override
    public void onDestroy(){
        //TODO Auto-generated method stub
        super.onDestroy();
        mediaPlayer.stop();
    }
}
