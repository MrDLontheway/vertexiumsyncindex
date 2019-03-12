package com.wxstc.dl

import com.wxscistor.pojo.vertexium.{GraphEdge, GraphProperty, GraphVertex}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.util.SizeEstimator
import org.vertexium.Visibility

object First {


//  def myfilter: Graph[GraphVertex, GraphEdge] => Graph[GraphVertex, GraphEdge] = {
//
//  }

//  val myfilter: Graph[GraphVertex, GraphEdge] => Graph[VD2_, ED2_] = ???


  def mymap(vid:VertexId,vertex:GraphVertex):GraphVertex ={
    vertex.properties.put("name",new GraphProperty("name","xiaolan",Visibility.EMPTY))
    vertex
  }



  val mymapFun = (vid:VertexId,vertex:GraphVertex) => mymap(vid,vertex)

  val vprog = (vid:VertexId, gv:GraphVertex, d:Double) => {

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[1]");

    val jsparkContext = new JavaSparkContext(conf)
    val vg = new VertexiumGraphx().getGraphByVerName(jsparkContext,"vertexium")
    print("my service ==================")
    //552498928772186112

//    vg.mapVertices(mymapFun)
    val v2 = vg.mapVertices((x,y)=>{
      y.properties.put("name",new GraphProperty("name","xiaolan",Visibility.EMPTY))
      y
    })

    val v3 = v2.mapEdges(x=>{
      x.attr.properties.put("age",new GraphProperty("age",3333,Visibility.EMPTY))
      x.attr
    })

    val v4 = v3.filter(x=>{
      val newVer = x.vertices.filter(_._1==552498922883383296L)
      val newE =  x.edges.filter(_.dstId==552498930303107072L)
      Graph(newVer,newE)
    })

    //mast vertex and edge inner join
    val v5 = v4.mask(v3)

    // 合并相同边
    val v6 = v4.groupEdges((x,y)=>{
      if(x.fromKey==y.fromKey){
        y
      }else{
        y
      }
    })

    val t = Double.PositiveInfinity
    //def pregel[A : ClassTag](initialMsg: A,maxIterations: Int = Int.MaxValue,activeDirection: EdgeDirection = EdgeDirection.Either)(vprog: (graphx.VertexId, VD, A) => VD,sendMsg: EdgeTriplet[VD, ED] => scala.Iterator[(graphx.VertexId, A)],mergeMsg: (A, A) => A): Graph[VD, ED]

    /*val resultp = v4.pregel(t)(
      (id, dist, newDist) => {
        println("Vertex Program======="+newDist)
        dist
      }, // Vertex Program
      triplet=>{// Send Message
        println("triplet=====Send Message::"+triplet)
        Iterator((triplet.dstId, 1.0))
//        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
//          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
//          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
//        } else {
//          Iterator.empty
//        }
      },
      (a,b) => math.min(a,b) // Merge Message
    )

    printGraph(resultp)*/
    v3.cache()
    printGraph(v3)
    val res = SizeEstimator.estimate(v3)
    println(s"""SizeEstimator.estimate $res""")
    Thread.sleep(1000000000)
    jsparkContext.stop()
  }


  def printGraph(vg:Graph[GraphVertex, GraphEdge]): Unit ={
    vg.edges.take(20).foreach(x=>{
      println("edge:" + x+"================")
      println(x.attr.properties)
    })

    vg.vertices.take(20).foreach(x=>{
      println("vertex:" + x._1+"================")
      println(x._2.properties)
    })

    System.out.println("The number of Edges:  " + vg.ops.numEdges)
    System.out.println("The number of vertices:  " + vg.ops.numVertices)
  }
}
