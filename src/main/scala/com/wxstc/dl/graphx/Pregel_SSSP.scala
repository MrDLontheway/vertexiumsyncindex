package com.wxstc.dl.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @Author: dl
  * @Date: 2018/11/9 10:46
  * @Version 1.0
  */
object Pregel_SSSP {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Pregel_SSSP").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")

    // A graph with edge attributes containing distances
    val userid = users.map(x=>{x._1})
    val es = relationships.map(e=>{
      e.attr.toDouble
    })
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 5).mapEdges(e => e.attr.toDouble)
//    graph.edges.foreach(println)
    val r1 = GraphGenerators.logNormalGraph(sc, numVertices = 5)//.mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 0 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph : Graph[(Double, List[VertexId]), Double] = graph.mapVertices((id, _) =>
      if (id == sourceId) (0.0, List[VertexId](sourceId))
      else (Double.PositiveInfinity, List[VertexId]()))

    println("vertices=========>")
    initialGraph.vertices.collect().foreach(println(_))

    println("triplets=========>")
    initialGraph.triplets.collect().foreach(println(_))

    println("edges=========>")
    initialGraph.edges.collect().foreach(println(_))


    val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(

      // Vertex Program
      (id, dist, newDist) => {
        println("Vertex Program"+dist+"==========="+"newDist:::::::"+newDist)
        if (dist._1 < newDist._1) dist else newDist
      },

      // Send Message
      triplet => {
        println("Send Message========triplet:::::"+triplet)
        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr ) {
          val re = Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
          println("Send Message========"+(triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
          re
//          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          println("Send Message========   empty")
          Iterator.empty
        }
      },
      //Merge Message
      (a, b) => if (a._1 < b._1) a else b)

    val r = sssp.vertices.collect
    println(sssp.vertices.collect.mkString("\n"))
  }
}