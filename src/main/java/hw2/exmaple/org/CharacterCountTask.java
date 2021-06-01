package hw2.exmaple.org;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Посылаем строку, получаем строку для подсчета
 */
public class CharacterCountTask extends ComputeTaskAdapter<String, String> {

    @IgniteInstanceResource
    Ignite ignite;

    @NotNull
    @Override
    public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> nodes, String arg) {
        String[] count = arg.split(",");
        IgniteCache<Integer, Air> cache = ignite.cache("myCache");
        IgniteCache<Integer, Bible> cacheBible = ignite.getOrCreateCache("byble");

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


                            Bible cacheStrokaBible;
                            String airportOtByble;

                            String airportAirIn = cacheStrokaAir.getAirIn();
                            String airportAirOut = cacheStrokaAir.getAirOut();

                            String countryVmestoAirIn = null;
                            String countryVmestoAirOut = null;

                            //Заменяем аэропорты на страны
                            for (int y =1; y < Integer.parseInt(count[1]);y++){
                                boolean in = true;
                                boolean out = true;
                                cacheStrokaBible = cacheBible.get(y);
                                airportOtByble = cacheStrokaBible.getAirport();
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

                            // Округляем время до часов
                            int time = cacheStrokaAir.getTimestamp();
                            time = time - (time % 3600);
                            cacheStrokaAir.setAirIn(countryVmestoAirIn);
                            cacheStrokaAir.setAirOut(countryVmestoAirOut);
                            cacheStrokaAir.setTimestamp(time);

                            //Выводим на узлах обновленные записи (замена аэропорта на страну)
                            System.out.println(">>>cache - " + cacheStrokaAir);
                            System.out.println("-------------------------------------------");
                            return Integer.toString(time) + "," + cacheStrokaAir.getAirIn() +","+ cacheStrokaAir.getAirOut();
                        }
                    }
                    ,node);
        }

        return map;
    }

    //Подсчитываем количество вылетов из одной страны в другую за один час
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