package drz.tmdb.sync.timer;

import java.util.Timer;
import java.util.TimerTask;

public class MyTimer {

    public Timer timer;

    public long delay;//单位毫秒

    public long period;

    public int count;//定时器的执行次数

    public int maxCount;//执行次数上限

    public MyTimer( long delay, long period, int maxCount) {
        this.delay = delay;
        this.period = period;
        this.maxCount = maxCount;

        timer = new Timer();
        count = 0;
    }

    public void start(TimerTask task){
        timer.scheduleAtFixedRate(task,delay,period);
    }

    public void stop(){
        timer.cancel();
    }
}
