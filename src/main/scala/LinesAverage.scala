import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by Nagendra on 10/21/15.
 */
class LinesAverage {


  def countLineSize(rdd: RDD[String]): (Float) = {
    val rdds = rdd.map(token => token.length)
    val result  = computeAvg(rdds)
    val avg = result._1 / result._2.toFloat
    avg
  }

  def computeAvg(input: RDD[Int]) : (Int,Int)= {
    input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
      (x,y) => (x._1 + y._1, x._2 + y._2))
  }

}
