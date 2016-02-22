/**
 * Created by nagendra amalakanta on 2/22/2016.
 */
object MemoryTest extends App{

  val rdd = SparkCtx.sc.makeRDD(List("This is a test", "This is another test",
    "And yet another test"), 1)
  val counts = rdd.flatMap(line => {println(line); line.split(" ")})
  val words = counts.map(word => {println(word); (word,1)})
  val reducedWords = words.reduceByKey((x,y) => {println(s"$x+$y");x+y})
  reducedWords.collect()

  //Hold spark context for 20 seconds to check statistics in the UI. Not suggestable
  SparkCtx.holdSparkContext(20000)
}
