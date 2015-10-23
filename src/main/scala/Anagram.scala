import org.apache.spark.rdd.RDD

/**
 * Created by Nagendra on 10/23/15.
 */
class Anagram(name:String) extends Serializable {

  def collectAnagrams(rdd : RDD[String]): RDD[String] = {

    return rdd.flatMap(line => line.split("\\s+")).filter(x=>verifyAnagrams(x,name));
  }

  /**
   * Method to verify given strings are anagram or not
   * Time complexity is O(n)
   * @param str1 - The First String
   * @param str2 - The Second String
   * @return - Boolean value of strings anagram or not
   */
  def verifyAnagrams(str1 : String, str2 : String): Boolean = {
    if(str1.length != str2.length) {
      return false;
    }
    val letters = Array.fill[Int](256)(0);
    for(i <- 0 until str1.length) {

      letters(str1.charAt(i).toInt)+=1;
      letters(str2.charAt(i).toInt)-=1;
    }

    for(i <-0 until 256) {
      if(letters(i) != 0) {
        return false;
      }
    }
    return true;
  }

}
