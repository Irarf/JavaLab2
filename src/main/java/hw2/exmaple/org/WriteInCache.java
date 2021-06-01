package hw2.exmaple.org;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

import java.io.*;

public class WriteInCache {

    public String writeAir(Ignite ignite,String files){
        String countAir = null;
        IgniteCache<Integer, Air> cache = ignite.getOrCreateCache("myCache");

        try {
            int i = 1;
            //"/root/lab2/input"
            File file = new File(files);
            FileReader fr = new FileReader(file);
            BufferedReader reader = new BufferedReader(fr);
            String line = reader.readLine();
            while (line !=null){
                String[] words = line.split(",",0);
                cache.put(i, new Air(Integer.parseInt(words[0]),Integer.parseInt(words[1]),words[2],words[3]));
                line = reader.readLine();
                i++;
            }

            countAir = Integer.toString(i);
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return countAir;
    }
    public String writeBible(Ignite ignite,String fileBible){
        IgniteCache<Integer, Bible> cacheBible = ignite.getOrCreateCache("byble");
        String countBible = null;
        try {

            int i = 1;
            //"/root/lab2/ttttt.txt"
            File file1 = new File(fileBible);
            FileReader fr1 = new FileReader(file1);
            BufferedReader reader1 = new BufferedReader(fr1);
            String line1 = reader1.readLine();
            while (line1 !=null){
                String[] words1 = line1.split("\t");
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
        return countBible;
    }
void hellogriz(){
        System.out.println("hello");
}

}
