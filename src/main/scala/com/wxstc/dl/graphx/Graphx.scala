package com.wxstc.dl.graphx

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.vertexium.Vertex
import org.vertexium.accumulo.mapreduce.AccumuloVertexInputFormat

//
object Graphx {
  System.setProperty("HADOOP_USER_NAME", "root")
  val conf = new SparkConf().setAppName("graphxtest").setMaster("local[*]")
  //创建SparkContext，该对象是提交spark App的入口
  val sc = new SparkContext(conf)
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //vertex()
    lean()
    sc.stop()
  }

  def vertex(): Unit ={
    val job = new Job(sc.hadoopConfiguration)
//    val mg = new MyVertexiumConfig()
//    mg.AccumuloVertexInputFormatConfig(job)
//    AccumuloVertexInputFormat.setInputInfo(job, graph, AccumuloConnector.instanceName, AccumuloConnector.zooServers,
//      "root", new PasswordToken("123456"), Array[String]("vis22", "vis1"))
    val vertexRdd = sc.newAPIHadoopRDD(job.getConfiguration,classOf[AccumuloVertexInputFormat], classOf[Text], classOf[Vertex])
    vertexRdd.map(x=>{
      var pros = Map[String,Object]()
      val p1 = x._2.getProperties.iterator()
      while (p1.hasNext){
        val pro = p1.next()
        pros += (pro.getName->pro.getValue)
      }
      (x._1.toString.toLong,pros)
    })
    println(vertexRdd.collect().toBuffer)
  }

  def printGraph(g: Graph[VertexId, Int]): Unit = {

    println("vertices=========>")
    g.vertices.collect().foreach(println(_))

    println("triplets=========>")
    g.triplets.collect().foreach(println(_))

    println("edges=========>")
    g.edges.collect().foreach(e=>{println(e.attr.toDouble)})
  }

  def t1(): Unit ={
    val r1 = GraphGenerators.logNormalGraph(sc, numVertices = 5)
    printGraph(r1)
  }

  def lean(): Unit ={
    //hdfs://nameservice1/masters
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")

    val graph:Graph[(String, String), String] = Graph(users, relationships, defaultUser)

//    val ranks = graph.pageRank(0.0001).vertices

//    println(ranks.collect().toBuffer)

    // Count all users which are postdocs
    val count1 = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    // Count all the edges where src > dst
    val count2 = graph.edges.filter(e => e.srcId > e.dstId)

    graph.mapVertices((id,x)=>{
    })

    val short = ShortestPaths.run(graph,Array(3l))

    val vs = short.vertices.collect().toBuffer
    val es = short.edges.collect().toBuffer
    val ts = short.triplets.collect().toBuffer
    println("vertices==========>")
    vs.map(println(_))

    println("edges==========>")
    es.map(println(_))

    println("triplets==========>")
    ts.map(println(_))

    val ops = short.ops

    val re = short.mapVertices((v,sp)=>{
      println(v)
      println(sp)
      (v,sp)
    })


//    println(s"""vertices:$vs""")
//    println(s"""edges:$es""")
//    println(s"""triplets:$ts""")
//    println(s"""ops:$ops""")

//    println(count2.collect().toBuffer)
  }

}