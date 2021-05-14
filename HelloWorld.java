package hw2.exmaple.org;

import org.apache.ignite.*;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class HW2 {

    public static void main(String[] args) throws IgniteException {

        String fileCache = args[0];
        String fileInput = args[1];
        String fileBible = args[2];


        // Подготовка IgniteConfiguration для использования Java APIs
        IgniteConfiguration cfg = new IgniteConfiguration();


        DataStorageConfiguration storageCnf = new DataStorageConfiguration();

        storageCnf.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        //"/root/lab2/cache"
        storageCnf.setStoragePath(fileCache);

        //   // Запуск узла, как клиента.
        cfg.setClientMode(true);

        // Classes of custom Java logic will be transferred over the wire from this app.
        cfg.setPeerClassLoadingEnabled(true);

        // Setting up an IP Finder to ensure the client can locate the servers.
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        // Запуск узла
        Ignite ignite = Ignition.start(cfg);

        // Создание IgniteCache и присвоение ему значений.
        IgniteCache<Integer, Air> cache = ignite.getOrCreateCache("myCache");
        IgniteCache<Integer, Bible> cacheBible = ignite.getOrCreateCache("byble");

        String countAir = null;
        String countBible = null;
        String countItog;
        try {
            int i = 1;
            //"/root/lab2/input"
            File file = new File(fileInput);
            FileReader fr = new FileReader(file);
            BufferedReader reader = new BufferedReader(fr);
            String line = reader.readLine();


            while (line !=null){
                String[] words = line.split(",",0);
                //String stroka =words[0] + "," + words[1] + "," + words[2] + ","+ words[3];
                cache.put(i, new Air(Integer.parseInt(words[0]),Integer.parseInt(words[1]),words[2],words[3]));
                //cache.put(i, new Air(1,2,"words[2]","words[3]"));
                line = reader.readLine();
                i++;
            }

            countAir = Integer.toString(i);
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            int i = 1;
            //"/root/lab2/ttttt.txt"
            File file1 = new File(fileBible);
            FileReader fr1 = new FileReader(file1);
            BufferedReader reader1 = new BufferedReader(fr1);
            String line1 = reader1.readLine();


            while (line1 !=null){
                String[] words1 = line1.split("\t");
                //String stroka1 =words1[0] + "," + words1[1] ;
                cacheBible.put(i, new Bible(words1[0],words1[1]));
                line1 = reader1.readLine();
                i++;
            }
            countBible = Integer.toString(i);

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        countItog = countAir + "," + countBible;

        /*
        ArrayList<String> lines = new ArrayList<>();
        try(Scanner scan = new Scanner((new File("/root/lab2/input")))) {

            while (scan.hasNextLine()){
                 lines.add(scan.nextLine());
            }

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String[] mass = lines.toArray(new String[0]);*/
        IgniteCompute compute = ignite.compute();

        // Execute the task on the cluster and wait for its completion.
        compute.execute(CharacterCountTask.class, countItog);
        // System.out.println(">>> Total number of characters in the phrase is '" + cnt + "'.");
        //  System.out.println(">> Created the cache and add the values.");

        // Executing custom Java compute task on server nodes.
        // ignite.compute(ignite.cluster().forServers()).broadcast(new RemoteTask());

        System.out.println(">> Compute task is executed, check for output on the server nodes.");

        // Disconnect from the cluster.
        cache.destroy();
        cacheBible.destroy();
        ignite.close();
    }



    public static class CharacterCountTask extends ComputeTaskAdapter<String, String> {
        // 1. Splits the received string into words
        // 2. Creates a child job for each word
        // 3. Sends the jobs to other nodes for processing.
        @IgniteInstanceResource
        Ignite ignite;

        @NotNull
        @Override
        public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> nodes, String arg) {
            String[] count = arg.split(",");
            IgniteCache<Integer, Air> cache = ignite.cache("myCache");
            IgniteCache<Integer, Bible> cacheBible = ignite.getOrCreateCache("byble");
/*
                ArrayList<String> lines = new ArrayList<>();
                try(Scanner scan = new Scanner(new File("/root/lab2/ttttt.txt"))){
                    while(scan.hasNextLine()){
                        lines.add(scan.nextLine());
                    }
                }catch (FileNotFoundException e){
                    e.printStackTrace();
                }

                String[] array = lin    es.toArray(new String[0]);
*/
            //for (int i =1; i < 30;i++)
            //System.out.println(">>> Printing '" + cacheBible.get(i) + "'   ");
            //List<ComputeJob> jobs = new ArrayList<>();
            Map<ComputeJob,ClusterNode> map = new HashMap<>();

            Iterator<ClusterNode> it = nodes.iterator();

            for (int i =1; i < Integer.parseInt(count[0]);i++) {
                Air cacheStrokaAir = cache.get(i);

                if (!it.hasNext())
                    it = nodes.iterator();
                ClusterNode node = it.next();

                int finalI = i;
                map.put(new ComputeJobAdapter() {
                            @Nullable
                            @Override
                            public Object execute() {
                                //System.out.println(">>> Printing Id = " + finalI + " words = " + words[1] + " bible = " );
/*
                            // Округляем часы
                            int intReturn = Integer.parseInt(words[1]);
                            intReturn = intReturn - (intReturn % 3600);
                            words[1] = Long.toString(intReturn);
                            System.out.println(">>> Printing modif'" + words[1] + "' on from compute job.");
                            //Заменяем аэропорты на страны
*/

                                Bible cacheStrokaBible;
                                String airportOtByble;

                                String airportAirIn = cacheStrokaAir.getAirIn();
                                String airportAirOut = cacheStrokaAir.getAirOut();

                                String countryVmestoAirIn = null;
                                String countryVmestoAirOut = null;
                                //System.out.println(">>> airportAirIn - " + airportAirIn + "; airportAirOut - " + airportAirOut);
                                //System.out.println(">>>airport - " + airportAirOut + "; country - " + countryVmestoAirOut);
                                for (int y =1; y < Integer.parseInt(count[1]);y++){
                                    boolean in = true;
                                    boolean out = true;
                                    cacheStrokaBible = cacheBible.get(y);
                                    airportOtByble = cacheStrokaBible.getAirport();
                                    //System.out.println(">>> airportAirIn - " + airportAirOut + "; airportOtByble - " + airportOtByble);
                                    if (airportOtByble.equals(airportAirOut) && out){
                                        countryVmestoAirOut = cacheStrokaBible.getCountry();
                                        out = false;
                                    }
                                    if (airportOtByble.equals(airportAirIn) && in){
                                        countryVmestoAirIn = cacheStrokaBible.getCountry();
                                        in = false;
                                    }
                                    if (!out && !in)
                                        break;

                                }

                                int time = cacheStrokaAir.getTimestamp();
                                time = time - (time % 3600);
                                cacheStrokaAir.setAirIn(countryVmestoAirIn);
                                cacheStrokaAir.setAirOut(countryVmestoAirOut);
                                cacheStrokaAir.setTimestamp(time);

                                // System.out.println(">>>airportIn - " + airportAirIn + "; countryIn - " + countryVmestoAirIn);
                                // System.out.println(">>>airportOut - " + airportAirOut + "; countryOut - " + countryVmestoAirOut);
                                System.out.println(">>>cache - " + cacheStrokaAir);
                                System.out.println("-------------------------------------------");
                                return Integer.toString(time) + "," + cacheStrokaAir.getAirIn() +","+ cacheStrokaAir.getAirOut();
                            }
                        }
                        ,node);
            }

            return map;
        }

        @Override
        public String reduce(List<ComputeJobResult> results) {
            int sum = 0;

            Map<String,Integer> counter = new HashMap<>();
            for (ComputeJobResult res : results) {
                int newValue = counter.getOrDefault(res.getData(),0) +1;
                counter.put(res.getData(),newValue);
            }
            System.out.println(">>> results on from compute job" + counter);
            return null;
        }
    }


    public static class Air{
        private int id;
        private int timestamp;
        private String airIn;
        private String airOut;

        Air(int id,int timestamp, String airIn, String airOut){
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

    public static class Bible{
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

}
