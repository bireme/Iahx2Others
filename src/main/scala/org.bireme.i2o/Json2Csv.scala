package org.bireme.i2o

import java.io.{BufferedOutputStream,File,FileOutputStream,ObjectOutputStream}
import java.nio.charset.Charset
import java.nio.file.{Files,Paths}

import org.apache.commons.csv._

import scala.collection.immutable.{TreeMap,TreeSet}

class Json2Csv {
  val defNullCell = "<empty>"

  def toCsv(it: () => Iterator[Option[Map[String,Any]]],
            outFile: String,
            parameters: Map[String,String]): Unit = {
    val outEncoding = parameters.getOrElse("outEncoding", "utf-8")
    val fieldDelim = parameters.getOrElse("fieldDelim", ",").charAt(0)
    val explode = parameters.getOrElse("explodeFields", "").trim.split(" *, *")
                                                      .filter(!_.isEmpty).toSet
    val output = Files.newBufferedWriter(new File(outFile).toPath(),
                                                   Charset.forName(outEncoding))
    val csv = new CSVPrinter(output, CSVFormat.DEFAULT.withDelimiter(fieldDelim))
    val max = getMaxFldOcc(it(), explode)
    val header = getHeader(max)
    val fields = List() ++ max.keys

    header.foreach(csv.print(_))
    csv.println()

    it().foreach {
      case Some(doc) => {
        fields.foreach {
          fld => if (doc.contains(fld)) {
            val field = if (doc(fld) == null) "" else doc(fld) // field can be null
            printField(field, csv, max(fld))
          } else {
            if (explode(fld)) for (i <- 0 until max(fld)) csv.print(defNullCell)
            else csv.print(defNullCell)
          }
        }
        csv.println()
      }
      case None => ()
    }
    csv.close()
    output.close()
  }

  private def printField(fld: Any,
                         csv: CSVPrinter,
                         times: Int): Unit = {
    fld match {
      case lst:List[_] => {
        if (times == 1) csv.print(getString(lst, false))
        else {
          lst.foreach(x => csv.print(getString(x)))
          for (i <- lst.size until times) csv.print(defNullCell)
        }
      }
      case e:Any => {
        csv.print(getString(e))
        for (i <- 1 until times) csv.print(defNullCell)
      }
    }
  }

  private def getString(elem: Any,
                        unique: Boolean = true): String = {
    elem match {
      case map:Map[String,Any] => map.foldLeft[String]("") {
        case(s,e) => {
          s +
          (if (s.isEmpty) if (unique) "" else "{" else "|") +
          e._1 + ":" + getString(e._2, false)
        }
      } + (if (unique) "" else "}")
      case lst:List[Any] => lst.foldLeft[String]("") {
        case(s,e) => {
          s +
          (if (s.isEmpty) if (unique) "" else "[" else "|") +
          getString(e, false)
        }
      } + (if (unique) "" else "]")
      case x:Any => x.toString()
    }
  }

  private def getMaxFldOcc(docs: Iterator[Option[Map[String,_]]],
                           explode: Set[String]): Map[String,Int] = {
    docs.foldLeft[Map[String,Int]](TreeMap()) {
      case(map,Some(doc)) => doc.foldLeft[Map[String,Int]](map) {
        case(map2,(k,v)) => {
          val max = map2.getOrElse(k, 0)
          val qtt = v match {
            case lst:List[_] => if (explode(k)) lst.size else 1
            case _ => 1
          }
          if (qtt> max) map2 + ((k, qtt)) else map2
        }
      }
      case(map,None) => map
    }
  }

  private def getHeader(max: Map[String,Int]): List[String] = {
    max.foldLeft[List[String]](List()) {
      case (lst, (k,v)) => {
        if (v < 2) lst :+ k
        else (1 to v).foldLeft[List[String]](lst) {
          case (lst2,i) => lst2 :+ (k + "[" + i + "]")
        }
      }
    }
  }
}
