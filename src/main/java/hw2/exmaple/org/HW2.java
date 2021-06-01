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

/**
 *
 */
public class HW2 {

    /**
     *
     */
    public static void main(String[] args) throws IgniteException {

        String fileCache = args[0];
        String fileInput = args[1];
        String fileBible = args[2];


       // Air air = new Air();


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

        WriteInCache cashe = new WriteInCache();


        // Создание IgniteCache и присвоение ему значений.

        String countAir = null;
        String countBible = null;
        String countItog;
        //Записываем в кэш задание

        countAir = cashe.writeAir(ignite,fileInput);
        countBible = cashe.writeBible(ignite,fileBible);


        //Записали количество строк в обоих кэшах
        countItog = countAir + "," + countBible;

        IgniteCompute compute = ignite.compute();

        // Запускаем задачу
        compute.execute(CharacterCountTask.class, countItog);

        System.out.println(">> Compute task is executed, check for output on the server nodes.");

        // Сбрасываем кэш
       // cache.destroy();
       // cacheBible.destroy();
        ignite.close();
    }








}
