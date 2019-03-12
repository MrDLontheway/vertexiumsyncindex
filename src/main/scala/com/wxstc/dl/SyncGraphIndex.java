package com.wxstc.dl;

import com.wxscistor.concurrent.MGraphDBManager;
import com.wxscistor.config.VertexiumConfig;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.appender.rolling.RolloverStrategy;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.vertexium.Edge;
import org.vertexium.Vertex;
import org.vertexium.accumulo.AccumuloAuthorizations;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.AccumuloGraphConfiguration;
import org.vertexium.accumulo.mapreduce.AccumuloEdgeInputFormat;
import org.vertexium.accumulo.mapreduce.AccumuloVertexInputFormat;

import java.util.ArrayList;
import java.util.Map;

public class SyncGraphIndex {

    public static void main(String[] args) throws Exception {
//        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName(SyncGraphIndex.class.getName());
        conf.set("spark.driver.userClassPathFirst","true");
        conf.set("spark.yarn.user.classpath.first","true");
        conf.set("spark.executor.userClassPathFirst ","true");
        String file = LogManager.class.getProtectionDomain().getCodeSource().getLocation().getFile();
        String file2 = RolloverStrategy.class.getProtectionDomain().getCodeSource().getLocation().getFile();
//        conf.setMaster("spark://sinan04:7077");
//        conf.setJars(new String[]{
//                "/Users/daile/Desktop/Code/mayun/vertexium/target/vertexium-0.1.jar",
//        "/Users/daile/Desktop/Code/mayun/vertexiumsyncindex/target/vertexium-syncindex-1.0-SNAPSHOT.jar"});
//        conf.setMaster("local[*]");
//        conf.setMaster("yarn-client");
//        conf.set("spark.yarn.jar", "hdfs://nameservice1/user/root/spark-yarn_2.11-2.2.0-cdh6.0.0.jar");
//        conf.set("spark.yarn.scheduler.heartbeat.interval-ms","30000");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.setLogLevel("info");
        String instanceName = VertexiumConfig.properties.getProperty("accumuloInstanceName","accumulo");
        String user = VertexiumConfig.properties.getProperty("username","root");
        String passwd = VertexiumConfig.properties.getProperty("password","123456");
        String zookeepers = VertexiumConfig.properties.getProperty("zookeeperServers");
        String tablesName = VertexiumConfig.properties.getProperty("TABLE_NAME_PREFIX");

        String outTablesName = "vertexium";
        if(args.length==0){
            System.out.println("tablesName not null");
            sparkContext.stop();
        }
        tablesName = args[0];
        outTablesName = tablesName;
        int bx = Integer.valueOf(args[1]);
        Map map = VertexiumConfig.setTableName(tablesName);
        AccumuloGraphConfiguration graphConfig = new AccumuloGraphConfiguration(map);
        AccumuloGraph graph = AccumuloGraph.create(graphConfig);

        Configuration entries = sparkContext.hadoopConfiguration();
        entries.set("graph", "org.vertexium.accumulo.AccumuloGraph");
        map.forEach((x,y)->{
            entries.set("graph" + "." + x.toString(),y.toString());
        });

        Job job = new Job(sparkContext.hadoopConfiguration());
        Job job2 = new Job(sparkContext.hadoopConfiguration());

        Authorizations userAuthorizations = graph.getConnector().securityOperations().getUserAuthorizations(user);
        String[] auths = userAuthorizations.toString().split(",");
        AccumuloVertexInputFormat.setInputInfo(job, graph, instanceName, zookeepers,
                user, new PasswordToken(passwd), auths);

        AccumuloEdgeInputFormat.setInputInfo(job2, graph,instanceName, zookeepers,
                user, new PasswordToken(passwd), auths);

        JavaPairRDD<Text, Vertex> vertexPairRDD = sparkContext.newAPIHadoopRDD(job.getConfiguration(),
                AccumuloVertexInputFormat.class, Text.class, Vertex.class);

        JavaPairRDD<Text, Edge> edgePairRDD = sparkContext.newAPIHadoopRDD(job2.getConfiguration(),
                AccumuloEdgeInputFormat.class, Text.class, Edge.class);

        JavaRDD<Vertex> vertexJavaRDD = vertexPairRDD.map(x -> {
            return x._2;
        });

        JavaRDD<Edge> edgeJavaRDD = edgePairRDD.map(x -> {
            return x._2;
        });

//        JavaRDD<String> verids = vertexPairRDD.map(x -> {
//            return x._1.toString();
//        }).repartition(bx);
//
//        JavaRDD<String> edgeids = edgePairRDD.map(x -> {
//            return x._1.toString();
//        }).repartition(bx);
//
        String finalOutTablesName = outTablesName;

//        verids.foreachPartition((x)->{
//            ArrayList<String> ids = new ArrayList<>();
//            x.forEachRemaining(row->{
//                ids.add(row);
//            });
//            AccumuloGraph accumuloGraph = MGraphDBManager.getAccumuloGraph(finalOutTablesName);
//            Iterable<Vertex> vertices = accumuloGraph.getVertices(ids, new AccumuloAuthorizations(auths));
//            accumuloGraph.getSearchIndex().addElements(accumuloGraph,vertices,new AccumuloAuthorizations(auths));
//            accumuloGraph.flush();
//        });
//
//        edgeids.foreachPartition((x)->{
//            ArrayList<String> ids = new ArrayList<>();
//            x.forEachRemaining(row->{
//                ids.add(row);
//            });
//            AccumuloGraph accumuloGraph = MGraphDBManager.getAccumuloGraph(finalOutTablesName);
//            Iterable<Edge> edges = accumuloGraph.getEdges(ids, new AccumuloAuthorizations(auths));
//            accumuloGraph.getSearchIndex().addElements(accumuloGraph,edges,new AccumuloAuthorizations(auths));
//            accumuloGraph.flush();
//        });

        vertexJavaRDD.foreachPartition((x)->{
            AccumuloGraph accumuloGraph = MGraphDBManager.getAccumuloGraph(finalOutTablesName);
            ArrayList<Vertex> arr = new ArrayList<>();
            while (x.hasNext()){
                Vertex next = x.next();
                if(next!=null){
                    arr.add(next);
                    if(arr.size()>=100000) {
                        accumuloGraph.getSearchIndex().addElements(accumuloGraph,arr,new AccumuloAuthorizations(auths));
                        accumuloGraph.flush();
                        arr = new ArrayList<>();
                    }
                }
            }
        });

        edgeJavaRDD.foreachPartition((x)->{
            AccumuloGraph accumuloGraph = MGraphDBManager.getAccumuloGraph(finalOutTablesName);
            ArrayList<Edge> arr = new ArrayList<>();
            while (x.hasNext()){
                Edge next = x.next();
                if(next!=null){
                    arr.add(next);
                    if(arr.size()>=100000) {
                        accumuloGraph.getSearchIndex().addElements(accumuloGraph,arr,new AccumuloAuthorizations(auths));
                        accumuloGraph.flush();
                        arr = new ArrayList<>();
                    }
                }

            }
        });

//        long count = vertexJavaRDD.count();
//        long count2 = edgeJavaRDD.count();
//
//        System.out.println("vertexPairRDD Sync:"+count);
//        System.out.println("edgePairRDD Sync:"+count2);
        sparkContext.stop();
    }
}
