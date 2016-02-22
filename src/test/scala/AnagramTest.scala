import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by Nagendra Amalakanta on 02/02/16.
 */


class AnagramTest extends FunSuite with BeforeAndAfter {

  //var sc: SparkContext = _
  before {
    //val conf = new SparkConf().setMaster("local").setAppName("anagarm of string")
   // sc = new SparkContext(conf)
  }

  test("Anagram string check in a file") {
    val anagramToken : String = "tunring"
    // @@ SETUP

    // @@ EXERCISE
    val anagrams =  Anagram.collectAnagrams(anagramToken,SparkCtx.sc.textFile(getClass.getClassLoader.getResource("word_count_input.txt").getPath))
    // @@ VERIFY
    assert(anagrams.collect().toSet.size == 1)
  }
}
