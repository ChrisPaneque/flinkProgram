package master;

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
}
