import java.util.RandomAccess

import scala.collection.immutable

object tt {
  def main(args: Array[String]): Unit = {
    val ints: immutable.Seq[Int] = List(1)

    println(ints(0))

  }

}
