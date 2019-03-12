package com.wxstc.dl.graphx

import org.apache.spark.graphx.Graph

class VertexProperty(){
  case class UserProperty(val name: String) extends VertexProperty
  case class ProductProperty(val name: String, val price: Double) extends VertexProperty
  // The graph might then have the type:
  var graph: Graph[VertexProperty, String] = null
}
