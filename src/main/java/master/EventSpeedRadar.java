package master;

import org.apache.flink.api.java.tuple.Tuple6;

public class EventSpeedRadar extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> {

   public EventSpeedRadar() {}

    public EventSpeedRadar(Event event) {
        this.f0 = event.f0; //time
        this.f1 = event.f1; //vid
        this.f2 = event.f3; //xWay
        this.f3 = event.f6; //seg
        this.f4 = event.f5; //dir
        this.f5 = event.f2; //spd
    }
}
