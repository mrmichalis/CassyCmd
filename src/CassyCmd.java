/**
 * User: michalis
 * Date: 28/11/12
 * Time: 10:07
 * Version: 0.1
 *
 * Description:
 * Generate Cassandra JMX stats into a file so we can monitor via Zabbix.
 * Compatible with Cassandra 1.1.6
 *
 */

import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassyCmd {

    private static final String DEFAULT_HOST = "192.168.56.102";
    private static final int DEFAULT_PORT = 7199;

    private NodeProbe probe;
    private static final Logger logger = LoggerFactory.getLogger(CassyCmd.class);

    public CassyCmd(NodeProbe probe)
    {
        this.probe = probe;
    }

    public void printInfo()
    {
        boolean gossipInitialized = probe.isInitialized();
        log(String.format("%-17s: %s", "Token", probe.getToken()));
        log(String.format("%-17s: %s", "Gossip active", gossipInitialized));
        log(String.format("%-17s: %s", "Thrift active", probe.isThriftServerRunning()));
        log(String.format("%-17s: %s", "Load", probe.getLoadString()));
        if (gossipInitialized)
            log(String.format("%-17s: %s", "Generation No", probe.getCurrentGenerationNumber()));
        else
            log(String.format("%-17s: %s", "Generation No", 0));

        // Uptime
        long secondsUp = probe.getUptime() / 1000;
        log(String.format("%-17s: %d", "Uptime (seconds)", secondsUp));

        // Memory usage
        MemoryUsage heapUsage = probe.getHeapMemoryUsage();
        double memUsed = (double)heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double)heapUsage.getMax() / (1024 * 1024);
        log(String.format("%-17s: %.2f / %.2f", "Heap Memory (MB)", memUsed, memMax));

        // Data Center/Rack
        log(String.format("%-17s: %s", "Data Center", probe.getDataCenter()));
        log(String.format("%-17s: %s", "Rack", probe.getRack()));

        // Exceptions
        log(String.format("%-17s: %s", "Exceptions", probe.getExceptionCount()));

        CacheServiceMBean cacheService = probe.getCacheServiceMBean();

        // Key Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
        log(String.format("%-17s: size %d (bytes), capacity %d (bytes), %d hits, %d requests, %.3f recent hit rate, %d save period in seconds",
                "Key Cache",
                cacheService.getKeyCacheSize(),
                cacheService.getKeyCacheCapacityInBytes(),
                cacheService.getKeyCacheHits(),
                cacheService.getKeyCacheRequests(),
                cacheService.getKeyCacheRecentHitRate(),
                cacheService.getKeyCacheSavePeriodInSeconds()));

        // Row Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
        log(String.format("%-17s: size %d (bytes), capacity %d (bytes), %d hits, %d requests, %.3f recent hit rate, %d save period in seconds",
                "Row Cache",
                cacheService.getRowCacheSize(),
                cacheService.getRowCacheCapacityInBytes(),
                cacheService.getRowCacheHits(),
                cacheService.getRowCacheRequests(),
                cacheService.getRowCacheRecentHitRate(),
                cacheService.getRowCacheSavePeriodInSeconds()));
    }

    public void printThreadPoolStats()
    {
        log(String.format("%-25s%10s%10s%15s%10s%18s", "Pool Name", "Active", "Pending", "Completed", "Blocked", "All time blocked"));

        Iterator<Map.Entry<String, JMXEnabledThreadPoolExecutorMBean>> threads = probe.getThreadPoolMBeanProxies();
        while (threads.hasNext())
        {
            Entry<String, JMXEnabledThreadPoolExecutorMBean> thread = threads.next();
            String poolName = thread.getKey();
            JMXEnabledThreadPoolExecutorMBean threadPoolProxy = thread.getValue();
            log(String.format("%-25s%10s%10s%15s%10s%18s",
                    poolName,
                    threadPoolProxy.getActiveCount(),
                    threadPoolProxy.getPendingTasks(),
                    threadPoolProxy.getCompletedTasks(),
                    threadPoolProxy.getCurrentlyBlockedTasks(),
                    threadPoolProxy.getTotalBlockedTasks()));
        }
    }

    public void printColumnFamilyStats()
    {
        Map <String, List <ColumnFamilyStoreMBean>> cfstoreMap = new HashMap <String, List <ColumnFamilyStoreMBean>>();

        // get a list of column family stores
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe.getColumnFamilyStoreMBeanProxies();

        while (cfamilies.hasNext())
        {
            Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
            String tableName = entry.getKey();
            ColumnFamilyStoreMBean cfsProxy = entry.getValue();

            if (!cfstoreMap.containsKey(tableName))
            {
                List<ColumnFamilyStoreMBean> columnFamilies = new ArrayList<ColumnFamilyStoreMBean>();
                columnFamilies.add(cfsProxy);
                cfstoreMap.put(tableName, columnFamilies);
            }
            else
            {
                cfstoreMap.get(tableName).add(cfsProxy);
            }
        }

        // print out the table statistics
        for (Entry<String, List<ColumnFamilyStoreMBean>> entry : cfstoreMap.entrySet())
        {
            String tableName = entry.getKey();
            List<ColumnFamilyStoreMBean> columnFamilies = entry.getValue();
            long tableReadCount = 0;
            long tableWriteCount = 0;
            int tablePendingTasks = 0;
            double tableTotalReadTime = 0.0f;
            double tableTotalWriteTime = 0.0f;

            log(String.format("Keyspace: " + tableName));
            for (ColumnFamilyStoreMBean cfstore : columnFamilies)
            {
                long writeCount = cfstore.getWriteCount();
                long readCount = cfstore.getReadCount();

                if (readCount > 0)
                {
                    tableReadCount += readCount;
                    tableTotalReadTime += cfstore.getTotalReadLatencyMicros();
                }
                if (writeCount > 0)
                {
                    tableWriteCount += writeCount;
                    tableTotalWriteTime += cfstore.getTotalWriteLatencyMicros();
                }
                tablePendingTasks += cfstore.getPendingTasks();
            }

            double tableReadLatency = tableReadCount > 0 ? tableTotalReadTime / tableReadCount / 1000 : Double.NaN;
            double tableWriteLatency = tableWriteCount > 0 ? tableTotalWriteTime / tableWriteCount / 1000 : Double.NaN;

            log(String.format("\tRead Count: " + tableReadCount));
            log(String.format("\tRead Latency: " + String.format("%s", tableReadLatency) + " ms."));
            log(String.format("\tWrite Count: " + tableWriteCount));
            log(String.format("\tWrite Latency: " + String.format("%s", tableWriteLatency) + " ms."));
            log(String.format("\tPending Tasks: " + tablePendingTasks));

            // print out column family statistics for this table
            for (ColumnFamilyStoreMBean cfstore : columnFamilies)
            {
                log(String.format("\t\tColumn Family: " + cfstore.getColumnFamilyName()));
                log(String.format("\t\tSSTable count: " + cfstore.getLiveSSTableCount()));
                log(String.format("\t\tSpace used (live): " + cfstore.getLiveDiskSpaceUsed()));
                log(String.format("\t\tSpace used (total): " + cfstore.getTotalDiskSpaceUsed()));
                log(String.format("\t\tNumber of Keys (estimate): " + cfstore.estimateKeys()));
                log(String.format("\t\tMemtable Columns Count: " + cfstore.getMemtableColumnsCount()));
                log(String.format("\t\tMemtable Data Size: " + cfstore.getMemtableDataSize()));
                log(String.format("\t\tMemtable Switch Count: " + cfstore.getMemtableSwitchCount()));
                log(String.format("\t\tRead Count: " + cfstore.getReadCount()));
                log(String.format("\t\tRead Latency: " + String.format("%01.3f", cfstore.getRecentReadLatencyMicros() / 1000) + " ms."));
                log(String.format("\t\tWrite Count: " + cfstore.getWriteCount()));
                log(String.format("\t\tWrite Latency: " + String.format("%01.3f", cfstore.getRecentWriteLatencyMicros() / 1000) + " ms."));
                log(String.format("\t\tPending Tasks: " + cfstore.getPendingTasks()));
                log(String.format("\t\tBloom Filter False Postives: " + cfstore.getBloomFilterFalsePositives()));
                log(String.format("\t\tBloom Filter False Ratio: " + String.format("%01.5f", cfstore.getRecentBloomFilterFalseRatio())));
                log(String.format("\t\tBloom Filter Space Used: " + cfstore.getBloomFilterDiskSpaceUsed()));
                log(String.format("\t\tCompacted row minimum size: " + cfstore.getMinRowSize()));
                log(String.format("\t\tCompacted row maximum size: " + cfstore.getMaxRowSize()));
                log(String.format("\t\tCompacted row mean size: " + cfstore.getMeanRowSize()));

                log(String.format(""));
            }
            log(String.format("----------------"));
        }
    }

    private void log(String s) {
        logger.info(String.format("[%-15s] %s",DEFAULT_HOST,s));
    }

    public static void main(String[] args) throws IOException, InterruptedException, ConfigurationException, ParseException{
        String username = "";
        String password = "";
        NodeProbe probe = null;
        try
        {
            probe = username == null ? new NodeProbe(DEFAULT_HOST, DEFAULT_PORT) : new NodeProbe(DEFAULT_HOST, DEFAULT_PORT, username, password);
        }
        catch (IOException ioe)
        {
                System.err.printf("Cannot resolve '%s': unknown host\n", new Object[] { DEFAULT_HOST });
                System.exit(1);
        }
        CassyCmd cassyCmd = new CassyCmd(probe);
        cassyCmd.printInfo();
        cassyCmd.printThreadPoolStats();
        cassyCmd.printColumnFamilyStats();
        System.exit(0);
    }
}
