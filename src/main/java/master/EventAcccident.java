package master;

import org.apache.flink.api.java.tuple.Tuple7;

public class EventAcccident extends Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

    public EventAcccident() {
    }

    public EventAcccident(Event first, Event fourth) {
        this.f0 = first.get("time"); //time1
        this.f1 = fourth.get("time"); //time2
        this.f2 = first.get("vid"); //vid
        this.f3 = first.get("xWay"); //xWay
        this.f4 = first.get("seg"); //seg
        this.f5 = first.get("dir"); //dir
        this.f6 = first.get("pos"); //pos
    }
}
