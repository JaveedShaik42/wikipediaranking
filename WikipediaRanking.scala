package wikipedia

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */ 
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Wikipedia")

  val sc: SparkContext = new SparkContext(conf)

// TASK 1
///////////////////////////////////////////////////////////////////////////////////////
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(WikipediaData.parse).persist() // Done


  //TASK2
/////////////////////////////////////////////////////////////////////////////////////////
///
// TASK 2: attempt #1 ----------------------------------------------------------

  /** Returns the number of articles in which the language `lang` occurs.
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    val x = rdd.aggregate(0)((acc, page) => if(page.text.contains(lang)) acc +1 else acc, (acc1, acc2) => acc1 + acc2)
    x
  }
 /** Uses `occurrencesOfLang` to compute the ranking of the languages
    * (`val langs`) by determining the number of Wikipedia articles that
    * mention each language at least once.
    *
    * IMPORTANT: The result is sorted by number of occurrences, in descending order.
    */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    val x = langs.map(lang => (lang, occurrencesOfLang(lang, rdd))).sortBy(-_._2)
    x
  }

  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = { // TODO: check how to split into different steps
    rdd.flatMap(article => langs.filter(lang => article.text.split(" ").contains(lang)).map(lang => (lang, article))).groupByKey()
  }

  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    val x = index.map({case (lang, articles) => (lang, articles.size)})
    x.sortBy(-_._2).collect().toList
    
  }    
  def zipLangWithPoint(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Int)] = {
    val x = rdd.flatMap(article => langs.filter(lang => article.text.contains(lang)).map(lang => (lang, 1)))
    x
  }

  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = zipLangWithPoint(langs, rdd).reduceByKey(_ + _).sortBy(-_._2).collect().toList
  
  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}


  @main def run =
    import WikipediaRanking._
  
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Languages ranked according to (1)
    val langsRanked: List[(String, Int)] =
      timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    // An inverted index mapping languages to wikipedia pages on which they appear
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    // Languages ranked according to (2), using the inverted index
    val langsRanked2: List[(String, Int)]
      = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    // Languages ranked according to (3)
    val langsRanked3: List[(String, Int)]
      = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    // Output the speed of each ranking
    println(timing)
    sc.stop()