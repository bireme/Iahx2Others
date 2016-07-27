package org.bireme.i2o

import java.io._
import java.nio.channels.FileChannel

class FileDocIterator(inFile: File) extends Iterator[Map[String,Any]] {
  val fis = new FileInputStream(inFile)
  val fc = fis.getChannel()
  val ois = new ObjectInputStream(fis)
  val fsize = fc.size()

  def close() = ois.close()

  override def hasNext(): Boolean = fc.position() < fsize - 1

  override def next(): Map[String,Any] = {
    if (hasNext) {
      ois.readObject().asInstanceOf[Map[String,Any]]
    } else throw new NoSuchElementException()
  }
}
