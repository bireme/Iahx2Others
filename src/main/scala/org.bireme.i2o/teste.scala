package org.bireme.i2o

import scala.util.parsing.json._
import scala.io._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons._

object teste extends App {
  val source = Source.fromFile("teste.json")
  val lines = try source.mkString finally source.close()
  val json = JSON.parseFull(lines).get.asInstanceOf[Map[String,Any]]
  val response = json("response").asInstanceOf[Map[String,Any]]
  val total = response("numFound").asInstanceOf[Double].toInt
  println(total)
  val docs = response("docs").asInstanceOf[List[Map[String,Any]]]
  println(docs)

  for (doc <- docs) {

    val builder = new MongoDBObjectBuilder() ++= doc
    val a = new MongoDBObject(builder.result())
    println(a)
    println()
  }
}
