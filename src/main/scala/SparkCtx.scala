import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by dhana on 2/20/2016.
 */
object SparkCtx {

    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"));

}
