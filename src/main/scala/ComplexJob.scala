import org.apache.spark.HashPartitioner

/**
 * Created by nagendra.amalakanta on 2/23/2016.
 */
object ComplexJob {

  def main(args: Array[String]) {

    val data1 = Array[(Int,Char)] (
      (1,'a'), (2, 'b'),
      (3, 'c'), (4, 'd'),
      (5, 'e'), (3, 'f'),
      (2, 'g'), (1, 'h'))

    val rangePairs1 = SparkCtx.sc.parallelize(data1,3)

    val hasPairs1 = rangePairs1.partitionBy(new HashPartitioner(3))

    val data2 = Array[(Int, String)]((1, "A"), (2, "B"),
      (3, "C"), (4, "D"))
    val pairs2 = SparkCtx.sc.parallelize(data2,2)

    val rangePairs2 = pairs2.map(x => (x._1,x._2.charAt(0)))

    val data3 = Array[(Int,Char)]((1, 'X'), (2, 'Y'))

    val rangePairs3 = SparkCtx.sc.parallelize(data3,2)

    val rangePairs = rangePairs2.union(rangePairs3)

    val result = hasPairs1.join(rangePairs)

    println(result.count())
    //result.foreachWith(i => i)((x, i) => println("[result " + i + "] " + x))

    result.foreach(i=> println("[Result " + i._1 +" ]" + i._2))

    println(result.toDebugString)

  }

}
