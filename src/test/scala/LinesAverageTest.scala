import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by Nagendra on 10/21/15.
 */
class LinesAverageTest extends FunSuite with BeforeAndAfter{

  var sc: SparkContext = _
  before {
    val conf = new SparkConf().setMaster("local").setAppName("Avg of all lines")
    sc = new SparkContext(conf)
  }

  test("Word Count on gutenberg file") {
    // @@ SETUP
    val LinesAverage = new LinesAverage

    // @@ EXERCISE
    val avg = LinesAverage.countLineSize(sc.textFile(getClass.getClassLoader.getResource("word_count_input.txt").getPath))

    // @@ VERIFY
    assert( math.round(avg) ==  math.round(45.588776))
  }

}
