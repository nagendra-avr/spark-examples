import org.apache.spark.rdd.RDD

/**
 *
 */
class WordCount {
  def countWords(rdd: RDD[String], splitPattern: String): RDD[(String, Int)] = {
    rdd.flatMap(record => record.split(splitPattern)).map(token => (token, 1)).reduceByKey((x, y) => x + y)
  }

  def countWords(rdd: RDD[String]): RDD[(String, Int)] = {
    countWords(rdd, "\\s+")
  }
}
