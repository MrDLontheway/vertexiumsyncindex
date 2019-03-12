package com.wxstc.dl.graphx

import org.vertexium.accumulo.AccumuloGraph

/**
  * vertexiumGraph 连接Manager
  * @Author: dl
  * @Date: 2018/11/27 17:25
  * @Version 1.0
  */
object MGraphDBManager{
  var graph:AccumuloGraph = _

  def getAccumuloGraph(config:java.util.Map[String,Object]): AccumuloGraph ={
    synchronized{
      if(graph==null){
//        import java.net.InetAddress
//        val ia = InetAddress.getLocalHost
//        val host = ia.getHostName //获取计算机主机名
//        val IP = ia.getHostAddress //获取计算机IP

        println(s"""======================================================================================================""")
        println(s"""==================================== create AccumuloGraph=============================================""")
        println(s"""======================================================================================================""")
        graph = AccumuloGraph.create(config)
      }
    }
    graph
  }
}
