import hw2.exmaple.org.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class test2 {

    @Test
    public void testCountTask(){
        //Air air = new Air(1,132124,"German","Norway");
        String[] file  = {"/root/lab2/cache", "/root/lab2/testInput","/root/lab2/ttttt.txt"};


        HW2.main(file);

    }
}
