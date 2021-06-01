import hw2.exmaple.org.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class test1 {
    private WriteInCache writeInCache;
    private IgniteConfiguration cfg;
    private  Ignite ignite;
    @Before
    public void initTeset(){
        writeInCache = new WriteInCache();
        cfg = new IgniteConfiguration();
        cfg.setClientMode(true);

        // Classes of custom Java logic will be transferred over the wire from this app.
        cfg.setPeerClassLoadingEnabled(true);

        // Setting up an IP Finder to ensure the client can locate the servers.
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));
        ignite = Ignition.start(cfg);
    }
    @Test
    public void testAir(){
        //Air air = new Air(1,132124,"German","Norway");
        Assert.assertEquals("Hello","31",writeInCache.writeAir(ignite,"/root/lab2/input"));
    }
}
