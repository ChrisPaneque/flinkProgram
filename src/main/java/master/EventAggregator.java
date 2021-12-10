package master;


public class EventAggregator {

    private Event first;
    private Event last;
    private int distance;
    private boolean isCompleted;

    public EventAggregator() {
        this.first = null;
        this.last = null;
        this.distance = 1;
        this.isCompleted = false;
    }

    public Event getFirst(){
        return first;
    }

    public Event getLast(){
        return last;
    }

    public boolean isCompleted(){
        return isCompleted;
    }

    public int getDstance(Event event){
        return Math.abs(this.first.get("pos") - event.get("pos"));
    }

    public void accumule(Event last){
        if(this.first == null){
            this.first = last;
        }
        this.isCompleted = (this.first.get("seg") == 56 && last.get("seg") == 52) || ( this.first.get("seg") == 52 && last.get("seg") == 56);
        if(this.isCompleted) {
            int dist = this.getDstance(last);
            if( dist >= this.distance){
                this.last = last;
                this.distance = dist;
            }
        }
    }
}
