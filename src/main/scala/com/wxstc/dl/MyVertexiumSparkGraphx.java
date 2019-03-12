package com.wxstc.dl;


import com.wxscistor.concurrent.MGraphDBManager;
import com.wxscistor.config.VertexiumConfig;
import com.wxscistor.pojo.vertexium.GraphEdge;
import com.wxscistor.pojo.vertexium.GraphProperty;
import com.wxscistor.pojo.vertexium.GraphVertex;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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
import org.vertexium.*;
import org.vertexium.accumulo.AccumuloAuthorizations;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.AccumuloGraphConfiguration;
import org.vertexium.accumulo.mapreduce.AccumuloEdgeInputFormat;
import org.vertexium.accumulo.mapreduce.AccumuloVertexInputFormat;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MyVertexiumSparkGraphx {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName(MyVertexiumSparkGraphx.class.getName()).setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        String instanceName = VertexiumConfig.properties.getProperty("accumuloInstanceName","accumulo");
        String user = VertexiumConfig.properties.getProperty("username","root");
        String passwd = VertexiumConfig.properties.getProperty("password","123456");
        String zookeepers = VertexiumConfig.properties.getProperty("zookeeperServers");
        String tableName = "mergevertexium";

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

        Job job = new Job(sparkContext.hadoopConfiguration());
        Job job2 = new Job(sparkContext.hadoopConfiguration());

        AccumuloVertexInputFormat.setInputInfo(job, graph, instanceName, zookeepers,
                user, new PasswordToken(passwd), new String[]{"vis22", "vis1"});

        AccumuloEdgeInputFormat.setInputInfo(job2, graph,instanceName, zookeepers,
                user, new PasswordToken(passwd), new String[]{"vis22", "vis1"});

        JavaPairRDD<Text, Vertex> vertexPairRDD = sparkContext.newAPIHadoopRDD(job.getConfiguration(),
                AccumuloVertexInputFormat.class, Text.class, Vertex.class);

        JavaPairRDD<Text, Edge> edgePairRDD = sparkContext.newAPIHadoopRDD(job2.getConfiguration(),
                AccumuloEdgeInputFormat.class, Text.class, Edge.class);

        JavaPairRDD<Vertex, Long> vertexLongJavaPairRDD = vertexPairRDD.map(Tuple2::_2).zipWithUniqueId();

        JavaPairRDD<Edge, Long> edgeLongJavaPairRDD = edgePairRDD.map(Tuple2::_2).zipWithUniqueId();

        JavaRDD<Tuple2<Object, GraphVertex>> vertexRDD =
                vertexPairRDD.map(x -> {
                    GraphVertex gv = new GraphVertex();
                    gv.setRowKey(x._2.getId());
                    gv.setVisibility(x._2.getVisibility());
                    x._2().getProperties().forEach(property->{
                        gv.properties.put(property.getName(),new GraphProperty(property.getKey(),property.getValue(),property.getVisibility()));
                    });
                    return new Tuple2(Long.valueOf(x._1.toString()), gv);//(AccumuloVertex) x._2);
                });

        Authorizations authA = new AccumuloAuthorizations("vis22", "vis1");

        JavaRDD<org.apache.spark.graphx.Edge<GraphEdge>> edgeRDD =
                edgePairRDD.map((Tuple2<Text, Edge> x) -> {
                    String out = x._2.getVertexId(Direction.OUT);
                    String in = x._2.getVertexId(Direction.IN);
                    GraphEdge ge = new GraphEdge();
                    ge.setVisibility(x._2.getVisibility());
                    ge.setRowKey(x._2.getId());
                    ge.fromKey = out;
                    ge.toKey = in;
                    ge.label = x._2.getLabel();
                    x._2.getProperties().forEach(property->{
                        ge.properties.put(property.getName(),new GraphProperty(property.getKey(),property.getValue(),property.getVisibility()));
                    });
                    org.apache.spark.graphx.Edge<GraphEdge> edge = new org.apache.spark.graphx.Edge(Long.valueOf(out), Long.valueOf(in), ge);
                    return edge;
                });

        Graph<GraphVertex, GraphEdge> graphx = Graph.<GraphVertex, GraphEdge>apply(vertexRDD.rdd(), edgeRDD.rdd(), null, StorageLevel.MEMORY_AND_DISK(),
                StorageLevel.MEMORY_AND_DISK(), ClassTag$.MODULE$.<GraphVertex>apply(GraphVertex.class), ClassTag$.MODULE$.<GraphEdge>apply(GraphEdge.class));

        //========================================java 调用spark scala api
//        graphx.edges().foreachPartition(new AbstractFunction1<Iterator<org.apache.spark.graphx.Edge<GraphEdge>>, BoxedUnit>() {
//            @Override
//            public BoxedUnit apply(Iterator<org.apache.spark.graphx.Edge<GraphEdge>> v1) {
//                while (v1.hasNext()){
//                    org.apache.spark.graphx.Edge<GraphEdge> next = v1.next();
//                    System.out.println("GraphEdge:"+next.attr.getId());
//                }
//                return null;
//            }
//        });

        Map mapConfig2 = new HashMap();
        mapConfig2.put(AccumuloGraphConfiguration.ACCUMULO_INSTANCE_NAME, "accumulo");
        mapConfig2.put(AccumuloGraphConfiguration.ACCUMULO_USERNAME, "root");
        mapConfig2.put(AccumuloGraphConfiguration.ACCUMULO_PASSWORD, "123456");
        mapConfig2.put(AccumuloGraphConfiguration.ZOOKEEPER_SERVERS, "sinan10:2181,sinan11:2181,sinan12:2181");
        //accumulo 数据库  图库TABLE_NAME_PREFIX
        mapConfig2.put(AccumuloGraphConfiguration.TABLE_NAME_PREFIX, "sparkvertexium");
//        saveGraph(mapConfig2,graphx);

        /*
        SingleGraph graph2 = new SingleGraph("visualizationDemo");
        // Set up the visual attributes for graph visualization
        graph2.addAttribute("ui.stylesheet","url(file:D:\\_vm\\stylesheet)");
        graph2.addAttribute("ui.quality");
        graph2.addAttribute("ui.antialias");

        JavaRDD<org.apache.spark.graphx.Edge<GraphEdge>> edges = graphx.edges().toJavaRDD();
        graphx.vertices().toJavaRDD().collect().forEach(x->{
            graph2.addNode(x._1().toString());
//            SingleNode n = (SingleNode) edges1;
        });
        edges.collect().forEach(x->{
            try {
                graph2.addEdge(x.attr.id, x.srcId() + "", x.dstId() + "", true);
            }catch (Exception e){

            }
        });
        graph2.display();*/

//        JavaRDD<Tuple2<Object, GraphVertex>> age2 = graphx.vertices().toJavaRDD().filter(x -> {
//            Object age = x._2.properties.getOrDefault("age", 0);
//            int age1 = (int) age;
//            if (age1 > 15)
//                return true;
//            else
//                return false;
//        });

//        age2.toArray().forEach(x->{
//            System.out.println("filter:"+x);
//        });

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

        sparkContext.stop();
    }

    public static void saveGraph(Map mapConfig,Graph<GraphVertex, GraphEdge> graph){
//        Predef.$eq$colon$eq<GraphVertex, GraphVertex> eq = Predef.$eq$colon$eq.tpEquals();

//        Graph<GraphVertex, GraphEdge> graphVertexGraphEdgeGraph = graph.mapVertices((x, y) -> {
//            return new GraphVertex();
//        }, ClassTag$.MODULE$.apply(GraphVertex.class), eq);

//        graph.vertices().toJavaRDD().foreachPartition(x->{
//            AccumuloGraph accumuloGraph = MGraphDBManager.getAccumuloGraph("");
//            x.forEachRemaining(y->{
//                accumuloGraph.addVertex(y._2().id,new Visibility(y._2.visibility),new AccumuloAuthorizations("vis22", "vis1"));
//            });
//            accumuloGraph.flush();
//        });
//
//        graph.edges().toJavaRDD().foreachPartition(x->{
//            AccumuloGraph accumuloGraph = MGraphDBManager.getAccumuloGraph("");
//            x.forEachRemaining(y->{
//                GraphEdge attr = y.attr;
//                Visibility visibility = Visibility.EMPTY;
//                if(attr.visibility!=null){
//                    visibility = new Visibility(attr.visibility);
//                }
//                EdgeBuilderByVertexId edgeBuilderByVertexId = accumuloGraph.prepareEdge(attr.id, attr.fromId, attr.toId, attr.label,visibility );
//                attr.getProperties().forEach((k,v)->{
//                    edgeBuilderByVertexId.setProperty(k,v,Visibility.EMPTY);
//                });
//                edgeBuilderByVertexId.save(new AccumuloAuthorizations("vis22", "vis1"));
//            });
//            accumuloGraph.flush();
//        });
    }
}
