package org.bireme.i2o

import java.io._
import java.util.{ArrayList,NoSuchElementException}

import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{DefaultHttpClient,HttpClientBuilder}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils

import scala.io.Source
import scala.util.parsing.json.JSON
import scala.collection.JavaConverters._

class DocumentIterator(query: String,
                 url: String = "http://db02dx.bireme.br:8983/portal-org/select",
                       from: Int = 0,
                       quantity: Int = -1,
                       outFile: Option[File] = None)
                                            extends Iterator[Map[String,Any]] {
  val url0 = url.trim
  val url_ = if (url0.endsWith("/")) url0 else url0 + "/"
  val query0 = query.trim
  val query_ = if (query0.charAt(0) == '@') {
      val source = Source.fromFile(query0.substring(1))
      try source.mkString finally source.close()
  } else query0
  val httpClient = HttpClientBuilder.create().build()
  val post = new HttpPost(url_)
  val numDocs = getNumDocs()
  val defQuantity = Integer.min(1000, numDocs)

  post.addHeader("Content-type", "application/x-www-form-urlencoded")
  post.addHeader("Accept", "application/json")
  /*post.addHeader("Accept-Charset", "utf-8")
  post.addHeader("Accept", "text/plain")
*/
  val last = if (quantity < 0) numDocs - 1
             else Integer.min(from + quantity - 1, numDocs - 1)
  val oos = outFile match {
    case Some(f) => Some(new ObjectOutputStream(
                         new BufferedOutputStream(
                         new FileOutputStream(f))))
    case None => None
 }
  var current = if (from < 0) 0 else from
  var docs = getDocuments(current, 1)

  def close(): Unit = oos match {
    case Some(o) => o.close()
    case _ => ()
  }

  override def hasNext(): Boolean = {
    current <= last
  }

  override def next(): Map[String,Any] = {
    if (hasNext) {
      val xdocs = docs match {
        case Nil => getDocuments(current, defQuantity)
        case _ => docs
      }
      docs = xdocs.tail
      if (current % 1000 == 0) println("+++" + current + "/" + numDocs)
      current += 1
      val doc = xdocs.head
      oos match {
        case Some(o) => o.writeObject(doc)
        case _ => ()
      }
      doc
    } else throw new NoSuchElementException()
  }

  private def getNumDocs(): Int = {
    val nvps = List[NameValuePair](new BasicNameValuePair("rows", "0"),
                                   new BasicNameValuePair("wt", "json"),
                                   new BasicNameValuePair("q", query_)).asJava
    post.setEntity(new UrlEncodedFormEntity(nvps))
    /*val str = url_ + "?x=x&rows=0&wt=json&q=" + query_
    val postingString = new StringEntity(str, "utf-8")
println("str=" + str)
    post.setEntity(postingString)*/

    val response = httpClient.execute(post);
    val entity = response.getEntity();
    val content = if (entity == null) "" else EntityUtils.toString(entity)

    if (content.isEmpty) 0
    else {
      val json = JSON.parseFull(content).get.asInstanceOf[Map[String,Any]]
      val response = json("response").asInstanceOf[Map[String,Any]]
      response("numFound").asInstanceOf[Double].toInt
    }
  }

  private def getDocuments(from: Int,
                           qtt: Int) : List[Map[String,Any]] = {
    if (from <= last) loadDocuments(from, qtt) else Nil
  }

  private def loadDocuments(from: Int,
                            qtt: Int): List[Map[String,Any]] = {
    //print("Loading documents from: " + from + " ... ")
    val rows = if ((from + qtt) <= last) qtt
               else (last - from + 1)
    val nvps = List[NameValuePair](new BasicNameValuePair("wt", "json"),
                                 new BasicNameValuePair("start", from.toString),
                                 new BasicNameValuePair("rows", rows.toString),
                                 new BasicNameValuePair("q", query_)).asJava
    post.setEntity(new UrlEncodedFormEntity(nvps))
    /* val str = url_ + "?x=x&wt=json&q=" + query_ + "&start=" + from + "&rows=" +
    val postingString = new StringEntity(str, "utf-8")
    post.setEntity(postingString)*/

    val response = httpClient.execute(post);
    val entity = response.getEntity();
    val content = if (entity == null) "" else EntityUtils.toString(entity)

    if (content.isEmpty) Nil
    else {
      val json = JSON.parseFull(content).get.asInstanceOf[Map[String,Any]]
      val response = json("response").asInstanceOf[Map[String,Any]]
      response("docs").asInstanceOf[List[Map[String,Any]]]
    }
  }
}
