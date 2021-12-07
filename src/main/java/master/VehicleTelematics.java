package master;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import java.nio.file.Paths;

/**
 * Java program using Flink for analysing mobility patterns of vehicles
 */
public class VehicleTelematics {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        String input = args.length >= 1 ?  args[0] : null;
        String output = args.length >= 2 ?  args[1] : null;

        // Checking input parameters
        Preconditions.checkNotNull(input, "Input DataStream should not be null.");
        Preconditions.checkNotNull(output, "Output should not be null.");

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataStream<String> inputStream = env.readTextFile(input);
        DataStream<Event> events = inputStream.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String line) {
                return new Event(line);
            }
        });

       //SingleOutputStreamOperator<Event> events = env.readTextFile(input).map(new EventParser());

        SingleOutputStreamOperator<EventSpeedRadar> speedRadar = events
                .filter(new SpeedRadarFilter())
                .map(new SpeedRadarMap());

        // emit result
        speedRadar.writeAsCsv(Paths.get(output, "speedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // execute program
        env.execute("Streaming Vehicle Telematics");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the EventParser that splits input data in Events
     */
    public static final class SpeedRadarMap implements MapFunction<Event, EventSpeedRadar> {

        @Override
        public EventSpeedRadar map(Event event) throws Exception {
            return new EventSpeedRadar(event);
        }
    }

    /**
     * Implements the EventParser that splits input data in Events
     */
    public static final class SpeedRadarFilter implements FilterFunction<Event> {

        @Override
        public boolean filter(Event event) throws Exception {
            return event.f2 > 90;
        }
    }

    /**
     * Implements the EventParser that splits input data in Events
     */
    public static final class EventParser implements MapFunction<String, Event> {

        @Override
        public Event map(String line) {
            return new Event(line);
        }
    }
}

/**
 * Events class to format the input
 */
class Event extends Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

    public Event(String line){
        String[] elements = line.split(",");
        this.f0 = Integer.parseInt(elements[0]);
        this.f1 = Integer.parseInt(elements[1]);
        this.f2 = Integer.parseInt(elements[2]);
        this.f3 = Integer.parseInt(elements[3]);
        this.f4 = Integer.parseInt(elements[4]);
        this.f5 = Integer.parseInt(elements[5]);
        this.f6 = Integer.parseInt(elements[6]);
        this.f7 = Integer.parseInt(elements[7]);
    }
}

class EventSpeedRadar extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> {

    public EventSpeedRadar(Event event){
        this.f0 = event.f0;
        this.f1 = event.f1;
        this.f2 = event.f3;
        this.f3 = event.f6;
        this.f4 = event.f5;
        this.f5 = event.f2;
    }
}
