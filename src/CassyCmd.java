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
import java.io.PrintStream;
import java.lang.management.MemoryUsage;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.commons.cli.*;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.tools.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassyCmd {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 7199;

    private NodeProbe probe;
    private static final Logger logger = LoggerFactory.getLogger(CassyCmd.class);

    private static final Pair<String, String> HOST_OPT = new Pair<String, String>("h", "host");
    private static final Pair<String, String> PORT_OPT = new Pair<String, String>("p", "port");

    private static ToolOptions options = null;
    static
    {
        options = new ToolOptions();
        options.addOption(HOST_OPT,         true, "node hostname or ip address");
        options.addOption(PORT_OPT,         true, "jmx agent port number");
    }

    private enum NodeCommand
    {
        CFSTATS,
        INFO,
        RING,
        TPSTATS,
        VERSION
    }

    public CassyCmd(NodeProbe probe) {
        this.probe = probe;
    }

    public void printRingCsv(String keyspace) {
        Map<String, String> tokenToEndpoint = probe.getTokenToEndpointMap();
        List<String> sortedTokens = new ArrayList<String>(tokenToEndpoint.keySet());

        Collection<String> liveNodes = probe.getLiveNodes();
        Collection<String> deadNodes = probe.getUnreachableNodes();
        Collection<String> joiningNodes = probe.getJoiningNodes();
        Collection<String> leavingNodes = probe.getLeavingNodes();
        Collection<String> movingNodes = probe.getMovingNodes();
        Map<String, String> loadMap = probe.getLoadMap();

        //String format = "\"%-16s\",\"%-12s\",\"%-12s\",\"%-7s\",\"%-8s\",\"%-16s\",\"%-20s\",\"%-44s\"";
        String format = "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"";

        // Calculate per-token ownership of the ring
        Map<String, Float> ownerships;
        try {
            ownerships = probe.effectiveOwnership(keyspace);
//            logger.info(String.format(format, "Address", "DC", "Rack", "Status", "State", "Load", "Effective-Ownership", "Token"));
        } catch (ConfigurationException ex) {
            ownerships = probe.getOwnership();
//            logger.info(String.format("Note: Ownership information does not include topology, please specify a keyspace. \n"));
//            logger.info(String.format(format, "Address", "DC", "Rack", "Status", "State", "Load", "Owns", "Token"));
        }

        // show pre-wrap token twice so you can always read a node's range as
        // (previous line token, current line token]
        if (sortedTokens.size() > 1)
            logger.info(String.format(format, "", "", "", "", "", "", "", sortedTokens.get(sortedTokens.size() - 1)));

        for (String token : sortedTokens) {
            String primaryEndpoint = tokenToEndpoint.get(token);
            String dataCenter;
            try {
                dataCenter = probe.getEndpointSnitchInfoProxy().getDatacenter(primaryEndpoint);
            } catch (UnknownHostException e) {
                dataCenter = "Unknown";
            }
            String rack;
            try {
                rack = probe.getEndpointSnitchInfoProxy().getRack(primaryEndpoint);
            } catch (UnknownHostException e) {
                rack = "Unknown";
            }
            String status = liveNodes.contains(primaryEndpoint)
                    ? "Up"
                    : deadNodes.contains(primaryEndpoint)
                    ? "Down"
                    : "?";

            String state = "Normal";

            if (joiningNodes.contains(primaryEndpoint))
                state = "Joining";
            else if (leavingNodes.contains(primaryEndpoint))
                state = "Leaving";
            else if (movingNodes.contains(primaryEndpoint))
                state = "Moving";

            String load = loadMap.containsKey(primaryEndpoint)
                    ? loadMap.get(primaryEndpoint)
                    : "?";
            String owns = new DecimalFormat("##0.00%").format(ownerships.get(token) == null ? 0.0F : ownerships.get(token));
            logger.info(String.format(format, primaryEndpoint, dataCenter, rack, status, state, load, owns, token));
        }
    }

    public void printInfoCsv() {
        //"Token","Gossip active","Thrift active","Load","Generation No","Uptime (seconds)","Heap Memory (MB) ","Data Center","Rack","Exceptions","KeyCacheSize","KeyCacheCapacityInBytes","KeyCacheHits","KeyCacheRequests","KeyCacheRecentHitRate","KeyCacheSavePeriodInSeconds","RowCacheSize","RowCacheCapacityInBytes","RowCacheHits","RowCacheRequests","RowCacheRecentHitRate","RowCacheSavePeriodInSeconds"
//        String format = "\"%-38s\",\"%-15s\",\"%-15s\",\"%-15s\",\"%-15s\",\"%-20s\",\"%-20s\",\"%-20s\",\"%-20s\",\"%-27s\",\"%-27s\",\"%-27s\",\"%-27s\",\"%-27s\",\"%-27s\",\"%-27s\",\"%-27s\",\"%-27s\",\"%-27s\",\"%-27s\",\"%-27s\"";
        String format = "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"";
        //logger.info(String.format(format,"Token","Gossip active","Thrift active","Load","Generation No","Uptime (seconds)","Heap Memory (MB) ","Data Center","Rack","Exceptions","KeyCacheSize","KeyCacheCapacityInBytes","KeyCacheHits","KeyCacheRequests","KeyCacheRecentHitRate","KeyCacheSavePeriodInSeconds","RowCacheSize","RowCacheCapacityInBytes","RowCacheHits","RowCacheRequests","RowCacheRecentHitRate","RowCacheSavePeriodInSeconds"));
        boolean gossipInitialized = probe.isInitialized();
        int generation = gossipInitialized
                ? probe.getCurrentGenerationNumber()
                : 0;

        // Uptime
        long secondsUp = probe.getUptime() / 1000;
        // Memory usage
        MemoryUsage heapUsage = probe.getHeapMemoryUsage();
        double memUsed = (double) heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double) heapUsage.getMax() / (1024 * 1024);
        String heapMemory = (String.format("%.2f / %.2f (MB)", memUsed, memMax));

        CacheServiceMBean cacheService = probe.getCacheServiceMBean();
        logger.info(String.format(format,
                probe.getToken(), gossipInitialized,
                probe.isThriftServerRunning(),
                probe.getLoadString(),
                generation,
                secondsUp,
                heapMemory,
                probe.getDataCenter(),
                probe.getRack(),
                probe.getExceptionCount(),
                cacheService.getKeyCacheSize(),
                cacheService.getKeyCacheCapacityInBytes(),
                cacheService.getKeyCacheHits(),
                cacheService.getKeyCacheRequests(),
                cacheService.getKeyCacheRecentHitRate(),
                cacheService.getKeyCacheSavePeriodInSeconds(),
                cacheService.getRowCacheSize(),
                cacheService.getRowCacheCapacityInBytes(),
                cacheService.getRowCacheHits(),
                cacheService.getRowCacheRequests(),
                cacheService.getRowCacheRecentHitRate(),
                cacheService.getRowCacheSavePeriodInSeconds()));
    }

    public void printThreadPoolStatsCsv() {
//        logger.info(String.format("%-25s%10s%10s%15s%10s%18s", "Pool Name", "Active", "Pending", "Completed", "Blocked", "All time blocked"));

        Iterator<Map.Entry<String, JMXEnabledThreadPoolExecutorMBean>> threads = probe.getThreadPoolMBeanProxies();
        while (threads.hasNext()) {
            Entry<String, JMXEnabledThreadPoolExecutorMBean> thread = threads.next();
            String poolName = thread.getKey();
            JMXEnabledThreadPoolExecutorMBean threadPoolProxy = thread.getValue();
//            logger.info(String.format("%-25s%10s%10s%15s%10s%18s",
            logger.info(String.format("%s,%s,%s,%s,%s,%s",
                    poolName,
                    threadPoolProxy.getActiveCount(),
                    threadPoolProxy.getPendingTasks(),
                    threadPoolProxy.getCompletedTasks(),
                    threadPoolProxy.getCurrentlyBlockedTasks(),
                    threadPoolProxy.getTotalBlockedTasks()));
        }
        logger.info("");
    }

    public void printColumnFamilyStatsCsv() {
        Map<String, List<ColumnFamilyStoreMBean>> cfstoreMap = new HashMap<String, List<ColumnFamilyStoreMBean>>();

        // get a list of column family stores
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe.getColumnFamilyStoreMBeanProxies();

        while (cfamilies.hasNext()) {
            Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
            String tableName = entry.getKey();
            ColumnFamilyStoreMBean cfsProxy = entry.getValue();

            if (!cfstoreMap.containsKey(tableName)) {
                List<ColumnFamilyStoreMBean> columnFamilies = new ArrayList<ColumnFamilyStoreMBean>();
                columnFamilies.add(cfsProxy);
                cfstoreMap.put(tableName, columnFamilies);
            } else {
                cfstoreMap.get(tableName).add(cfsProxy);
            }
        }
        // print out the table statistics
        for (Entry<String, List<ColumnFamilyStoreMBean>> entry : cfstoreMap.entrySet()) {
            String tableName = entry.getKey();
            List<ColumnFamilyStoreMBean> columnFamilies = entry.getValue();
            long tableReadCount = 0;
            long tableWriteCount = 0;
            int tablePendingTasks = 0;
            double tableTotalReadTime = 0.0f;
            double tableTotalWriteTime = 0.0f;


            //"Keyspace","Read Count","Read Latency","Write Count","Write Latency","Pending Tasks"
            String format = "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"";


//            log(String.format("Keyspace: " + tableName));
            for (ColumnFamilyStoreMBean cfstore : columnFamilies) {
                long writeCount = cfstore.getWriteCount();
                long readCount = cfstore.getReadCount();

                if (readCount > 0) {
                    tableReadCount += readCount;
                    tableTotalReadTime += cfstore.getTotalReadLatencyMicros();
                }
                if (writeCount > 0) {
                    tableWriteCount += writeCount;
                    tableTotalWriteTime += cfstore.getTotalWriteLatencyMicros();
                }
                tablePendingTasks += cfstore.getPendingTasks();
            }
            double tableReadLatency = tableReadCount > 0 ? tableTotalReadTime / tableReadCount / 1000 : Double.NaN;
            double tableWriteLatency = tableWriteCount > 0 ? tableTotalWriteTime / tableWriteCount / 1000 : Double.NaN;

            logger.info(String.format(format,"Keyspace: " + tableName,tableReadCount,tableReadLatency,tableWriteCount,tableWriteLatency,tablePendingTasks));
            // print out column family statistics for this table
//            for (ColumnFamilyStoreMBean cfstore : columnFamilies) {
//                log(String.format("\t\tColumn Family: " + cfstore.getColumnFamilyName()));
//                log(String.format("\t\tSSTable count: " + cfstore.getLiveSSTableCount()));
//                log(String.format("\t\tSpace used (live): " + cfstore.getLiveDiskSpaceUsed()));
//                log(String.format("\t\tSpace used (total): " + cfstore.getTotalDiskSpaceUsed()));
//                log(String.format("\t\tNumber of Keys (estimate): " + cfstore.estimateKeys()));
//                log(String.format("\t\tMemtable Columns Count: " + cfstore.getMemtableColumnsCount()));
//                log(String.format("\t\tMemtable Data Size: " + cfstore.getMemtableDataSize()));
//                log(String.format("\t\tMemtable Switch Count: " + cfstore.getMemtableSwitchCount()));
//                log(String.format("\t\tRead Count: " + cfstore.getReadCount()));
//                log(String.format("\t\tRead Latency: " + String.format("%01.3f", cfstore.getRecentReadLatencyMicros() / 1000) + " ms."));
//                log(String.format("\t\tWrite Count: " + cfstore.getWriteCount()));
//                log(String.format("\t\tWrite Latency: " + String.format("%01.3f", cfstore.getRecentWriteLatencyMicros() / 1000) + " ms."));
//                log(String.format("\t\tPending Tasks: " + cfstore.getPendingTasks()));
//                log(String.format("\t\tBloom Filter False Postives: " + cfstore.getBloomFilterFalsePositives()));
//                log(String.format("\t\tBloom Filter False Ratio: " + String.format("%01.5f", cfstore.getRecentBloomFilterFalseRatio())));
//                log(String.format("\t\tBloom Filter Space Used: " + cfstore.getBloomFilterDiskSpaceUsed()));
//                log(String.format("\t\tCompacted row minimum size: " + cfstore.getMinRowSize()));
//                log(String.format("\t\tCompacted row maximum size: " + cfstore.getMaxRowSize()));
//                log(String.format("\t\tCompacted row mean size: " + cfstore.getMeanRowSize()));
//
//                log(String.format(""));
//            }
//            log(String.format("----------------%n"));
        }
    }

    //region Original output
    public void printRing(String keyspace) {
        Map<String, String> tokenToEndpoint = probe.getTokenToEndpointMap();
        List<String> sortedTokens = new ArrayList<String>(tokenToEndpoint.keySet());

        Collection<String> liveNodes = probe.getLiveNodes();
        Collection<String> deadNodes = probe.getUnreachableNodes();
        Collection<String> joiningNodes = probe.getJoiningNodes();
        Collection<String> leavingNodes = probe.getLeavingNodes();
        Collection<String> movingNodes = probe.getMovingNodes();
        Map<String, String> loadMap = probe.getLoadMap();

        String format = "%-16s%-12s%-12s%-7s%-8s%-16s%-20s%-44s";

        // Calculate per-token ownership of the ring
        Map<String, Float> ownerships;
        try {
            ownerships = probe.effectiveOwnership(keyspace);
            log(String.format(format, "Address", "DC", "Rack", "Status", "State", "Load", "Effective-Ownership", "Token"));
        } catch (ConfigurationException ex) {
            ownerships = probe.getOwnership();
            log(String.format("Note: Ownership information does not include topology, please specify a keyspace. \n"));
            log(String.format(format, "Address", "DC", "Rack", "Status", "State", "Load", "Owns", "Token"));
        }

        // show pre-wrap token twice so you can always read a node's range as
        // (previous line token, current line token]
        if (sortedTokens.size() > 1)
            log(String.format(format, "", "", "", "", "", "", "", sortedTokens.get(sortedTokens.size() - 1)));

        for (String token : sortedTokens) {
            String primaryEndpoint = tokenToEndpoint.get(token);
            String dataCenter;
            try {
                dataCenter = probe.getEndpointSnitchInfoProxy().getDatacenter(primaryEndpoint);
            } catch (UnknownHostException e) {
                dataCenter = "Unknown";
            }
            String rack;
            try {
                rack = probe.getEndpointSnitchInfoProxy().getRack(primaryEndpoint);
            } catch (UnknownHostException e) {
                rack = "Unknown";
            }
            String status = liveNodes.contains(primaryEndpoint)
                    ? "Up"
                    : deadNodes.contains(primaryEndpoint)
                    ? "Down"
                    : "?";

            String state = "Normal";

            if (joiningNodes.contains(primaryEndpoint))
                state = "Joining";
            else if (leavingNodes.contains(primaryEndpoint))
                state = "Leaving";
            else if (movingNodes.contains(primaryEndpoint))
                state = "Moving";

            String load = loadMap.containsKey(primaryEndpoint)
                    ? loadMap.get(primaryEndpoint)
                    : "?";
            String owns = new DecimalFormat("##0.00%").format(ownerships.get(token) == null ? 0.0F : ownerships.get(token));
            log(String.format(format, primaryEndpoint, dataCenter, rack, status, state, load, owns, token));
        }
        log(String.format("%n"));
    }

    public void printInfo() {
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
        double memUsed = (double) heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double) heapUsage.getMax() / (1024 * 1024);
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

    public void printThreadPoolStats() {
        log(String.format("%-25s%10s%10s%15s%10s%18s", "Pool Name", "Active", "Pending", "Completed", "Blocked", "All time blocked"));

        Iterator<Map.Entry<String, JMXEnabledThreadPoolExecutorMBean>> threads = probe.getThreadPoolMBeanProxies();
        while (threads.hasNext()) {
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
        log(String.format("%n"));
    }

    public void printColumnFamilyStats() {
        Map<String, List<ColumnFamilyStoreMBean>> cfstoreMap = new HashMap<String, List<ColumnFamilyStoreMBean>>();

        // get a list of column family stores
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe.getColumnFamilyStoreMBeanProxies();

        while (cfamilies.hasNext()) {
            Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
            String tableName = entry.getKey();
            ColumnFamilyStoreMBean cfsProxy = entry.getValue();

            if (!cfstoreMap.containsKey(tableName)) {
                List<ColumnFamilyStoreMBean> columnFamilies = new ArrayList<ColumnFamilyStoreMBean>();
                columnFamilies.add(cfsProxy);
                cfstoreMap.put(tableName, columnFamilies);
            } else {
                cfstoreMap.get(tableName).add(cfsProxy);
            }
        }

        // print out the table statistics
        for (Entry<String, List<ColumnFamilyStoreMBean>> entry : cfstoreMap.entrySet()) {
            String tableName = entry.getKey();
            List<ColumnFamilyStoreMBean> columnFamilies = entry.getValue();
            long tableReadCount = 0;
            long tableWriteCount = 0;
            int tablePendingTasks = 0;
            double tableTotalReadTime = 0.0f;
            double tableTotalWriteTime = 0.0f;

            log(String.format("Keyspace: " + tableName));
            for (ColumnFamilyStoreMBean cfstore : columnFamilies) {
                long writeCount = cfstore.getWriteCount();
                long readCount = cfstore.getReadCount();

                if (readCount > 0) {
                    tableReadCount += readCount;
                    tableTotalReadTime += cfstore.getTotalReadLatencyMicros();
                }
                if (writeCount > 0) {
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
            for (ColumnFamilyStoreMBean cfstore : columnFamilies) {
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
            log(String.format("----------------%n"));
        }
    }

    private void log(String s) {
        //logger.info(String.format("[%-15s] %s",DEFAULT_HOST,s));
        logger.info(String.format("%s", s));
    }
    //endregion

    public static void main(String[] args) throws IOException, InterruptedException, ConfigurationException, ParseException {
        CommandLineParser parser = new PosixParser();
        ToolCommandLine cmd = null;

        String username = "";
        String password = "";

        try
        {
            cmd = new ToolCommandLine(parser.parse(options, args));
        }
        catch (ParseException p)
        {
            badUse(p.getMessage());
        }

        String host = cmd.hasOption(HOST_OPT.left) ? cmd.getOptionValue(HOST_OPT.left) : DEFAULT_HOST;
        int port = DEFAULT_PORT;

        String portNum = cmd.getOptionValue(PORT_OPT.left);
        if (portNum != null)
        {
            try
            {
                port = Integer.parseInt(portNum);
            }
            catch (NumberFormatException e)
            {
                throw new ParseException("Port must be a number");
            }
        }

        NodeProbe probe = null;
        try
        {
            probe = username == null ? new NodeProbe(host, port) : new NodeProbe(host, port, username, password);
        }
        catch (IOException ioe)
        {
            Throwable inner = findInnermostThrowable(ioe);
            if (inner instanceof ConnectException)
            {
                System.err.printf("Failed to connect to '%s:%d': %s\n", host, port, inner.getMessage());
                System.exit(1);
            }
            else if (inner instanceof UnknownHostException)
            {
                System.err.printf("Cannot resolve '%s': unknown host\n", host);
                System.exit(1);
            }
            else
            {
                System.err.printf("Error connecting to remote JMX agent!");
            }
        }
        try
        {
            NodeCommand command = null;

            try
            {
                command = cmd.getCommand();
            }
            catch (IllegalArgumentException e)
            {
                badUse(e.getMessage());
            }

            CassyCmd cassyCmd = new CassyCmd(probe);
            // Execute the requested command.
            String[] arguments = cmd.getCommandArguments();
            String tag;
            String columnFamilyName = null;

            switch (command)
            {
                case RING :
                    if (arguments.length > 0) { cassyCmd.printRingCsv(arguments[0]); }
                    else                      { cassyCmd.printRingCsv(null); };
                    break;
                case INFO            : cassyCmd.printInfoCsv(); break;
                case TPSTATS         : cassyCmd.printThreadPoolStatsCsv(); break;
                case CFSTATS         : cassyCmd.printColumnFamilyStatsCsv(); break;
                case VERSION         : cassyCmd.printReleaseVersion(System.out); break;
                default :
                    throw new RuntimeException("Unreachable code.");
            }
        }
        finally
        {
            if (probe != null)
            {
                try
                {
                    probe.close();
                }
                catch (IOException ex)
                {
                    // swallow the exception so the user will see the real one.
                }
            }
        }
        System.exit(0);
    }

    public void printReleaseVersion(PrintStream outs)
    {
        outs.println("ReleaseVersion: " + probe.getReleaseVersion());
    }
    private static Throwable findInnermostThrowable(Throwable ex)
    {
        Throwable inner = ex.getCause();
        return inner == null ? ex : findInnermostThrowable(inner);
    }

    // Command Line Options
    private static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        StringBuilder header = new StringBuilder();
        header.append("\nAvailable commands:\n");
        // No args
        addCmdHelp(header, "info", "Print node information (uptime, load, ...)");
        addCmdHelp(header, "cfstats", "Print statistics on column families");
        addCmdHelp(header, "tpstats", "Print usage statistics of thread pools");
        addCmdHelp(header, "version", "Print cassandra version");

        // One arg
        addCmdHelp(header, "ring [keyspace]", "Print information about the token ring for a given keyspace (for all keyspaces if it is not specified)");
        //addCmdHelp(header, "netstats [host]", "Print network information on provided host (connecting node by default)");

        String usage = String.format("java %s --host <arg> <command>%n", CassyCmd.class.getName());
        hf.printHelp(usage, "", options, "");
        System.out.println(header.toString());
    }
    private static void addCmdHelp(StringBuilder sb, String cmd, String description)
    {
        sb.append("  ").append(cmd);
        // Ghetto indentation (trying, but not too hard, to not look too bad)
        if (cmd.length() <= 20)
            for (int i = cmd.length(); i < 22; ++i) sb.append(" ");
        sb.append(" - ").append(description).append("\n");
    }
    private static void badUse(String useStr)
    {
        System.err.println(useStr);
        printUsage();
        System.exit(1);
    }
    private static class ToolOptions extends Options
    {
        public void addOption(Pair<String, String> opts, boolean hasArgument, String description)
        {
            addOption(opts, hasArgument, description, false);
        }

        public void addOption(Pair<String, String> opts, boolean hasArgument, String description, boolean required)
        {
            addOption(opts.left, opts.right, hasArgument, description, required);
        }

        public void addOption(String opt, String longOpt, boolean hasArgument, String description, boolean required)
        {
            Option option = new Option(opt, longOpt, hasArgument, description);
            option.setRequired(required);
            addOption(option);
        }
    }
    private static class ToolCommandLine
    {
        private final CommandLine commandLine;

        public ToolCommandLine(CommandLine commands)
        {
            commandLine = commands;
        }

        public Option[] getOptions()
        {
            return commandLine.getOptions();
        }

        public boolean hasOption(String opt)
        {
            return commandLine.hasOption(opt);
        }

        public String getOptionValue(String opt)
        {
            return commandLine.getOptionValue(opt);
        }

        public NodeCommand getCommand()
        {
            if (commandLine.getArgs().length == 0)
                throw new IllegalArgumentException("Command was not specified.");

            String command = commandLine.getArgs()[0];

            try
            {
                return NodeCommand.valueOf(command.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new IllegalArgumentException("Unrecognized command: " + command);
            }
        }

        public String[] getCommandArguments()
        {
            List params = commandLine.getArgList();

            if (params.size() < 2) // command parameters are empty
                return new String[0];

            String[] toReturn = new String[params.size() - 1];

            for (int i = 1; i < params.size(); i++)
                toReturn[i - 1] = (String) params.get(i);

            return toReturn;
        }
    }
}
