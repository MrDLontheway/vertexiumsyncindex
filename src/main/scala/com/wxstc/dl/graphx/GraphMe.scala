package com.wxstc.dl.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: dl
  * @Date: 2018/11/27 15:13
  * @Version 1.0
  */
object GraphMe {
  System.setProperty("HADOOP_USER_NAME", "root")
  val conf = new SparkConf().setAppName("graphxtest").setMaster("local[*]")
  //创建SparkContext，该对象是提交spark App的入口
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")

    val graph:Graph[(String, String), String] = Graph(users, relationships, defaultUser)


    val res = graph.mapVertices((x,y)=>{
      (x,y)
    })
  }

}
