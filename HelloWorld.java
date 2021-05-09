import org.apache.ignite.*;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import java.io.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class HelloWorld {

    public static void main(String[] args) throws IgniteException {



        // Подготовка IgniteConfiguration для использования Java APIs
        IgniteConfiguration cfg = new IgniteConfiguration();

        DataStorageConfiguration storageCnf = new DataStorageConfiguration();

        storageCnf.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        storageCnf.setStoragePath("/root/lab2/cache");

       // cfg.setDataStorageConfiguration(storageCnf);
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
        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("myCache");
        IgniteCache<Integer, String> cacheBible = ignite.getOrCreateCache("byble");

        String countAir = null;
        String countBible = null;
        String countItog;
        try {
            int i = 1;
            File file = new File("/root/lab2/input");
            FileReader fr = new FileReader(file);
            BufferedReader reader = new BufferedReader(fr);
            String line = reader.readLine();


            while (line !=null){
                String[] words = line.split(",");
                String stroka =words[0] + "," + words[1] + "," + words[2] + ","+ words[3];
                cache.put(i, stroka);
                line = reader.readLine();
                i++;
            }
            System.out.println("i =" + i);
            countAir = Integer.toString(i);
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            int i = 1;
            File file1 = new File("/root/lab2/ttttt.txt");
            FileReader fr1 = new FileReader(file1);
            BufferedReader reader1 = new BufferedReader(fr1);
            String line1 = reader1.readLine();


            while (line1 !=null){
                String[] words1 = line1.split("\t");
                String stroka1 =words1[0] + "," + words1[1] ;
                cacheBible.put(i, stroka1);
                line1 = reader1.readLine();
                i++;
            }
            countBible = Integer.toString(i);
            System.out.println("i =" + i);
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
        int cnt = compute.execute(CharacterCountTask.class, countItog);
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

    public class Air{
        private int id;
        private long timestamp;
        private String airIn;
        private String airOut;

        public Air(int id,long timestamp, String airIn, String airOut){
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
        public long getTimestamp(){
            return timestamp;
        }
        public void setTimestamp(long timestamp){
            this.timestamp = timestamp;
        }
    }


        public static class CharacterCountTask extends ComputeTaskSplitAdapter<String, Integer> {
            // 1. Splits the received string into words
            // 2. Creates a child job for each word
            // 3. Sends the jobs to other nodes for processing.
            @IgniteInstanceResource
            Ignite ignite;

            @Override
            public List<ComputeJob> split(int gridSize, String arg) {
                String[] count = arg.split(",");
                IgniteCache<Integer, String> cache = ignite.cache("myCache");
                IgniteCache<Integer, String> cacheBible = ignite.getOrCreateCache("byble");
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
                List<ComputeJob> jobs = new ArrayList<>();

                for (int i =1; i < Integer.parseInt(count[0]);i++) {
                    String stroka = cache.get(i);
                    //String stroka1 = cacheBible.get(i);
                    String[] words = stroka.split(",");
                   // String[] words1 = stroka1.split(",");
                    int finalI = i;
                    jobs.add(new ComputeJobAdapter() {
                        @Override
                        public Object execute() {
                            System.out.println(">>> Printing Id = " + finalI + " words = " + words[1] + " bible = " );

                            // Округляем часы
                            int intReturn = Integer.parseInt(words[1]);
                            intReturn = intReturn - (intReturn % 3600);
                            words[1] = Long.toString(intReturn);
                            System.out.println(">>> Printing modif'" + words[1] + "' on from compute job.");
                            //Заменяем аэропорты на страны



                            return intReturn;
                        }
                    }
                    );
                }

                return jobs;
            }

            @Override
            public Integer reduce(List<ComputeJobResult> results) {
                int sum = 0;

                for (ComputeJobResult res : results) {
                    sum += res.<Integer>getData();
                    System.out.println(">>> results on from compute job. res.<Integer>getData() " + res.<Integer>getData());
                }

                return sum;
            }
        }
    /**
     * A compute tasks that prints out a node ID and some details about its OS and JRE.
     * Plus, the code shows how to access data stored in a cache from the compute task.
     */
    private static class RemoteTask implements IgniteRunnable {
        @IgniteInstanceResource
        Ignite ignite;



        @Override public void run() {
            try {
                File file = new File("/root/lab2/ttttt.txt");
                FileReader fr = new FileReader(file);
                BufferedReader reader = new BufferedReader(fr);
                String line = reader.readLine();

                while (line !=null){
                    String[] words = line.split("\t");

                    line = reader.readLine();

                }

            }catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println(">> Executing the compute task");

            System.out.println(
                    "   Node ID: " + ignite.cluster().localNode().id() + "\n" +
                            "   OS: " + System.getProperty("os.name") +
                            "   JRE: " + System.getProperty("java.runtime.name"));

            IgniteCache<Integer, String> cache = ignite.cache("myCache");
            for (int i =1; i < 30;i++)
            System.out.println(">> " + cache.get(i));
        }
    }

}