package master;

import org.apache.flink.api.java.tuple.Tuple6;

public class EventAverage extends Tuple6<Integer, Integer, Integer, Integer, Integer, Float> {

    public EventAverage() {
    }
    public EventAverage(Integer timeFirst, Integer timeLast, Integer vid, Integer xWay, Integer dir, Float avgSpeed ) {
        this.f0 = timeFirst; //Time1
        this.f1 = timeLast; //Time2
        this.f2 = vid; //VID
        this.f3 = xWay; //XWay
        this.f4 = dir; //Dir
        this.f5 = avgSpeed; //AvgSpd
    }

    public EventAverage(Event first, Event last){
        this.f0 = first.get("time");
        this.f1 = last.get("time");
        this.f2 = first.get("vid");
        this.f3 = first.get("xWay");
        this.f4 = first.get("dir");
        this.f5 = 0.0f;

        boolean westbound = first.get("dir") == 1;

        //Complete segment for both directions
        if( (first.get("seg") == 56 && last.get("seg") == 52) ||
            (first.get("seg") == 52 && last.get("seg") == 56) ){
            float time = last.get("time") - first.get("time"); //seconds
            float distance = westbound ? (first.get("pos") - last.get("pos")) : (last.get("pos") - first.get("pos")); //meters
            this.f5 = (distance / time) * 2.23694f; //mps(meters per second) -> mph(miles per hour)
        }
    }

    public Float getAvg(){
        return this.f5;
    }



}
