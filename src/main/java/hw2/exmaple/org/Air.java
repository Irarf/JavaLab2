package hw2.exmaple.org;

/**
 * Шаблон для кэш-памяти Air
 */
public class Air{
    private int id;
    private int timestamp;
    private String airIn;
    private String airOut;

   public Air(int id,int timestamp, String airIn, String airOut){
        this.id = id;
        this.timestamp = timestamp;
        this.airIn = airIn;
        this.airOut = airOut;
    }
    public String getAirIn(){
        return airIn;
    }
    public void setAirIn(String airIn){
        this.airIn = airIn;
    }
    public String getAirOut(){
        return airOut;
    }
    public void setAirOut(String airOut){
        this.airOut = airOut;
    }
    public int getId(){
        return id;
    }
    public void setId(int id){
        this.id = id;
    }
    public int getTimestamp(){
        return timestamp;
    }
    public void setTimestamp(int timestamp){
        this.timestamp = timestamp;
    }
    @Override public String toString() {
        return "Air [id=" + id +" timestamp="+ timestamp +" airIn="+airIn+" airOut="+airOut+"]";
    }
}
