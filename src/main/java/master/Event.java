package master;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;

/**
 * Events class to format the input
 */
public class Event extends Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

    public Event() {}

    public Event(String line) {
        String[] elements = line.split(",");
        this.f0 = Integer.parseInt(elements[0]); //time
        this.f1 = Integer.parseInt(elements[1]); //vid
        this.f2 = Integer.parseInt(elements[2]); //spd
        this.f3 = Integer.parseInt(elements[3]); //xWay
        this.f4 = Integer.parseInt(elements[4]); //lane
        this.f5 = Integer.parseInt(elements[5]); //dir
        this.f6 = Integer.parseInt(elements[6]); //seg
        this.f7 = Integer.parseInt(elements[7]); //pos
    }

    public Integer get(String id){
        switch(id) {
            case "time":
                return this.f0;
            case "vid":
                return this.f1;
            case "spd":
                return this.f2;
            case "xWay":
                return this.f3;
            case "lane":
                return this.f4;
            case "dir":
                return this.f5;
            case "seg":
                return this.f6;
            case "pos":
                return this.f7;
            default:
                throw new IndexOutOfBoundsException(id);
        }
    }

    public Tuple3<Integer, Integer, Integer> getKeyForAverage(){
        return Tuple3.of(this.f1, this.f3, this.f5);//vid, xWay, dir
    }

    public Tuple2<Integer, Integer> getKeyForAccidents(){
        return Tuple2.of(this.f1, this.f7);//vid, pos
    }
}
