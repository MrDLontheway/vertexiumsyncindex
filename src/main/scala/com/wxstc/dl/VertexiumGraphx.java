package com.wxstc.dl;

import com.wxscistor.config.VertexiumConfig;
import com.wxscistor.pojo.vertexium.GraphEdge;
import com.wxscistor.pojo.vertexium.GraphProperty;
import com.wxscistor.pojo.vertexium.GraphVertex;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import org.vertexium.Direction;
import org.vertexium.Edge;
import org.vertexium.Vertex;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.AccumuloGraphConfiguration;
import org.vertexium.accumulo.mapreduce.AccumuloEdgeInputFormat;
import org.vertexium.accumulo.mapreduce.AccumuloVertexInputFormat;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.util.HashMap;
import java.util.Map;

public class VertexiumGraphx {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName(MyVertexiumSparkGraphx.class.getName()).setMaster("local[1]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        String instanceName = VertexiumConfig.properties.getProperty("accumuloInstanceName","accumulo");
        String user = VertexiumConfig.properties.getProperty("username","root");
        String passwd = VertexiumConfig.properties.getProperty("password","123456");
        String zookeepers = VertexiumConfig.properties.getProperty("zookeeperServers");
        String tableName = "vertexium";

        Configuration entries = sparkContext.hadoopConfiguration();
        Map mapConfig = new HashMap();
        mapConfig.put(AccumuloGraphConfiguration.ACCUMULO_INSTANCE_NAME, instanceName);
        mapConfig.put(AccumuloGraphConfiguration.ACCUMULO_USERNAME, user);
        mapConfig.put(AccumuloGraphConfiguration.ACCUMULO_PASSWORD, passwd);
        mapConfig.put(AccumuloGraphConfiguration.ZOOKEEPER_SERVERS, zookeepers);
        //accumulo 数据库  图库TABLE_NAME_PREFIX
        mapConfig.put(AccumuloGraphConfiguration.TABLE_NAME_PREFIX, tableName);

        entries.set("graph", "org.vertexium.accumulo.AccumuloGraph");
        mapConfig.forEach((x,y)->{
            entries.set("graph" + "." + x.toString(),y.toString());
        });
        AccumuloGraphConfiguration graphConfig = new AccumuloGraphConfiguration(mapConfig);
        AccumuloGraph graph = AccumuloGraph.create(graphConfig);
        Authorizations root = graph.getConnector().securityOperations().getUserAuthorizations("root");
        String[] split = root.toString().split(",");

        Job job = new Job(sparkContext.hadoopConfiguration());
        Job job2 = new Job(sparkContext.hadoopConfiguration());

        AccumuloVertexInputFormat.setInputInfo(job, graph, instanceName, zookeepers,
                user, new PasswordToken(passwd), split);

        AccumuloEdgeInputFormat.setInputInfo(job2, graph,instanceName, zookeepers,
                user, new PasswordToken(passwd), split);

        JavaPairRDD<Text, Vertex> vertexPairRDD = sparkContext.newAPIHadoopRDD(job.getConfiguration(),
                AccumuloVertexInputFormat.class, Text.class, Vertex.class);

        JavaPairRDD<Text, Edge> edgePairRDD = sparkContext.newAPIHadoopRDD(job2.getConfiguration(),
                AccumuloEdgeInputFormat.class, Text.class, Edge.class);

        JavaRDD<Tuple2<Object, GraphVertex>> vertexjavaRDD = vertexPairRDD
                .map(x->{
                    try {
                        Object longId = x._2().getPropertyValue("__LongId");
                        if(longId==null){
                            return null;
                        }
                        GraphVertex gv = new GraphVertex();
                        gv.setRowKey(x._2.getId());
                        gv.setVisibility(x._2.getVisibility());
                        x._2().getProperties().forEach(property->{
                            gv.properties.put(property.getName(),new GraphProperty(property.getKey(),property.getValue(),property.getVisibility()));
                        });
                        return new Tuple2<Object, GraphVertex>(longId,gv);
                    }catch (NullPointerException e){
                        return null;
                    }
                }).filter(x->{
                    return x!=null;
                });
//                .zipWithUniqueId()
//                .map((x) -> {
//                    return new Tuple2<>(x._2, x._1);
//                });

        JavaRDD<org.apache.spark.graphx.Edge<GraphEdge>> edgeJavaRDD = edgePairRDD.map((x) -> {
            String out = x._2.getVertexId(Direction.OUT);
            String in = x._2.getVertexId(Direction.IN);
            Object outLong = x._2.getPropertyValue("__OUTLongId");
            Object inLong = x._2.getPropertyValue("__INLongId");
            if (outLong == null || inLong == null) {
                return null;
            }
            GraphEdge ge = new GraphEdge();
            ge.setVisibility(x._2.getVisibility());
            ge.rowKey = x._2.getId();
            ge.fromKey = out;
            ge.toKey = in;
            ge.label = x._2.getLabel();
            x._2.getProperties().forEach(property -> {
                ge.properties.put(property.getName(),new GraphProperty(property.getKey(),property.getValue(),property.getVisibility()));
            });
            org.apache.spark.graphx.Edge<GraphEdge> edge = new org.apache.spark.graphx.Edge((Long) outLong, (Long) inLong, ge);
            return edge;
        }).filter(x -> {
            return x != null;
        });

        Graph<GraphVertex, GraphEdge> graphx = Graph.<GraphVertex, GraphEdge>apply(vertexjavaRDD.rdd(), edgeJavaRDD.rdd(), null, StorageLevel.MEMORY_AND_DISK(),
                StorageLevel.MEMORY_AND_DISK(), ClassTag$.MODULE$.<GraphVertex>apply(GraphVertex.class), ClassTag$.MODULE$.<GraphEdge>apply(GraphEdge.class));

        graphx.edges().toJavaRDD().take(20).forEach(x->{
            System.out.println("edges:"+x);
        });

        graphx.vertices().toJavaRDD().take(20).forEach(x->{
            System.out.println("vertices:"+x);
        });

        System.out.println("The number of Edges:  " + graphx.ops().numEdges());

        System.out.println("The number of vertices:  " + graphx.ops().numVertices());

//        System.out.println("The count based on the filter:  " + graphx.vertices().filter(new Filter()).count());
        System.out.println("filter=============================");
        Thread.sleep(1000000000);
        sparkContext.stop();
    }

    public Graph<GraphVertex,GraphEdge> getGraphByVerName(JavaSparkContext sparkContext,String tableName) throws Exception {
        String instanceName = VertexiumConfig.properties.getProperty("accumuloInstanceName","accumulo");
        String user = VertexiumConfig.properties.getProperty("username","root");
        String passwd = VertexiumConfig.properties.getProperty("password","123456");
        String zookeepers = VertexiumConfig.properties.getProperty("zookeeperServers");

        Configuration entries = sparkContext.hadoopConfiguration();
        Map mapConfig = new HashMap();
        mapConfig.put(AccumuloGraphConfiguration.ACCUMULO_INSTANCE_NAME, instanceName);
        mapConfig.put(AccumuloGraphConfiguration.ACCUMULO_USERNAME, user);
        mapConfig.put(AccumuloGraphConfiguration.ACCUMULO_PASSWORD, passwd);
        mapConfig.put(AccumuloGraphConfiguration.ZOOKEEPER_SERVERS, zookeepers);
        //accumulo 数据库  图库TABLE_NAME_PREFIX
        mapConfig.put(AccumuloGraphConfiguration.TABLE_NAME_PREFIX, tableName);

        entries.set("graph", "org.vertexium.accumulo.AccumuloGraph");
        mapConfig.forEach((x,y)->{
            entries.set("graph" + "." + x.toString(),y.toString());
        });
        AccumuloGraphConfiguration graphConfig = new AccumuloGraphConfiguration(mapConfig);
        AccumuloGraph graph = AccumuloGraph.create(graphConfig);
        Authorizations root = graph.getConnector().securityOperations().getUserAuthorizations("root");
        String[] split = root.toString().split(",");

        Job job = new Job(sparkContext.hadoopConfiguration());
        Job job2 = new Job(sparkContext.hadoopConfiguration());

        AccumuloVertexInputFormat.setInputInfo(job, graph, instanceName, zookeepers,
                user, new PasswordToken(passwd), split);

        AccumuloEdgeInputFormat.setInputInfo(job2, graph,instanceName, zookeepers,
                user, new PasswordToken(passwd), split);

        JavaPairRDD<Text, Vertex> vertexPairRDD = sparkContext.newAPIHadoopRDD(job.getConfiguration(),
                AccumuloVertexInputFormat.class, Text.class, Vertex.class);

        JavaPairRDD<Text, Edge> edgePairRDD = sparkContext.newAPIHadoopRDD(job2.getConfiguration(),
                AccumuloEdgeInputFormat.class, Text.class, Edge.class);

        JavaRDD<Tuple2<Object, GraphVertex>> vertexjavaRDD = vertexPairRDD
                .map(x->{
                    try {
                        Object longId = x._2().getPropertyValue("__LongId");
                        if(longId==null){
                            return null;
                        }
                        GraphVertex gv = new GraphVertex();
                        gv.setRowKey(x._2.getId());
                        gv.setVisibility(x._2.getVisibility());
                        x._2().getProperties().forEach(property->{
                            gv.properties.put(property.getName(),new GraphProperty(property.getKey(),property.getValue(),property.getVisibility()));
                        });
                        return new Tuple2<Object, GraphVertex>(longId,gv);
                    }catch (NullPointerException e){
                        return null;
                    }
                }).filter(x->{
                    return x!=null;
                });

        JavaRDD<org.apache.spark.graphx.Edge<GraphEdge>> edgeJavaRDD = edgePairRDD.map((x) -> {
            String out = x._2.getVertexId(Direction.OUT);
            String in = x._2.getVertexId(Direction.IN);
            Object outLong = x._2.getPropertyValue("__OUTLongId");
            Object inLong = x._2.getPropertyValue("__INLongId");
            if (outLong == null || inLong == null) {
                return null;
            }
            GraphEdge ge = new GraphEdge();
            ge.setVisibility(x._2.getVisibility());
            ge.rowKey = x._2.getId();
            ge.fromKey = out;
            ge.toKey = in;
            ge.label = x._2.getLabel();
            x._2.getProperties().forEach(property -> {
                ge.properties.put(property.getName(),new GraphProperty(property.getKey(),property.getValue(),property.getVisibility()));
            });
            org.apache.spark.graphx.Edge<GraphEdge> edge = new org.apache.spark.graphx.Edge((Long) outLong, (Long) inLong, ge);
            return edge;
        }).filter(x -> {
            return x != null;
        });

        Graph<GraphVertex, GraphEdge> graphx = Graph.apply(vertexjavaRDD.rdd(), edgeJavaRDD.rdd(), null, StorageLevel.MEMORY_AND_DISK(),
                StorageLevel.MEMORY_AND_DISK(), ClassTag$.MODULE$.<GraphVertex>apply(GraphVertex.class), ClassTag$.MODULE$.<GraphEdge>apply(GraphEdge.class));

        return graphx;
    }
}
