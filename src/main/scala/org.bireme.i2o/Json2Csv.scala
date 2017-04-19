package org.bireme.i2o

import java.io.{File, Writer}
import java.nio.charset.Charset
import java.nio.file.{Files,Paths}

import org.apache.commons.csv.{CSVFormat,CSVPrinter}

import scala.collection.immutable.{TreeMap,TreeSet}
import scala.collection.JavaConversions._

class Json2Csv(val parameters: Map[String,String]) {
  val defNullCell = ""

  def toCsv(writer: Writer): Unit = {
    val i22 = new Iahx2Mongo2Json(parameters)
    val it = i22.toJson()
    val fieldDelim = parameters.getOrElse("fieldDelim", ",").charAt(0)
    val explode = parameters.getOrElse("explodeFields", "").trim.split(" *, *")
                                                  .filter(!_.isEmpty).toSet
    val csv = new CSVPrinter(writer, CSVFormat.DEFAULT.withDelimiter(fieldDelim))
    val header = i22.getHeader()

println("header=" + header)

    header.foreach(csv.print(_))
    csv.println()

    it.foreach {
      case Some(doc) => writeFields(doc, explode, List(), csv)
      case None => ()
    }
  }

  private def writeFields(flds: Map[String,Any],
                          expl: Set[String],
                          prefix: List[String],
                          csv: CSVPrinter): Unit = {
    if (flds.isEmpty) {
      prefix.foreach(csv.print(_))
      csv.println()
      csv.flush()
    } else flds.head match {
      case (k,v) => v match {
        case lst:List[_] =>
          if (expl.contains(k))
            lst.foreach(elem =>
                   writeFields(flds.tail, expl, prefix :+ getString(elem), csv))
          else writeFields(flds.tail, expl, prefix :+ getString(lst), csv)
        case elem => writeFields(flds.tail, expl, prefix :+ getString(elem), csv)
      }
    }
  }

  private def getString(elem: Any,
                        unique: Boolean = true): String = {
    elem match {
      case map:Map[String,Any] =>
        map.foldLeft[String]("") {
          case(s,e) => {
            s +
            (if (s.isEmpty) if (unique) "" else "{" else "|") +
              e._1 + ":" + getString(e._2, false)
          }
        } + (if (unique) "" else "}")
      case lst:List[Any] =>
        lst.foldLeft[String]("") {
          case(s,e) => {
            s +
            (if (s.isEmpty) if (unique) "" else "[" else "|") +
            getString(e, false)
          }
        } + (if (unique) "" else "]")
      case x:Any => x.toString()
    }
  }
}
