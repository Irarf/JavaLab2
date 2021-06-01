package hw2.exmaple.org;

/**
 * Шаблон для кэш-памяти Bible
 */
public class Bible{
    private String airport;
    private String country;

    Bible(String airport, String country){

        this.airport = airport;
        this.country = country;
    }
    public String getAirport(){
        return airport;
    }

    public void setAirport(String airport){
        this.airport = airport;
    }
    public String getCountry(){
        return country;
    }
    public void setCountry(String country){
        this.country = country;
    }

    @Override public String toString() {
        return "Bible [airport=" + airport +" country="+ country +"]";
    }
}
