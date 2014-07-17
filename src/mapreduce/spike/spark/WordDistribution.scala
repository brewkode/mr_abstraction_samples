package mapreduce.spike.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.rddToPairRDDFunctions

class WordDistribution extends SparkApp {
  val lines = List(
    "KAMALIKULTURE Zaylie Sling-Back Nude",
    "Frye James Wallet Small Dark Brown Tumbled Full Grain",
    "Josef Seibel Cecily Leather Sandals For Women Castoro Capri",
    "Osiris Kids' NYC 83 Pre/Grd Lime/Black/Purple Lime/Black/Purple",
    "Everest Casual Cotton Satchel Bag Black Black",
    "SKECHERS Bravos Gray",
    "Men's K-Swiss Grancourt II White/Silver",
    "Ed Hardy Coralie Peep-Toe Wedge – Ladies",
    "Lacoste Dreyfus Mts Dark Blue/Grey",
    "Men's Frye Gregory Penny Saddle Tumbled Full Grain",
    "Women's ECCO Nephi 70 MM Ankle Bootie Black Leather",
    "Women's Sperry Top-Sider Angelfish Tan/Bronze Damask Floral",
    "Jeffrey Campbell 'Barnes' Bootie WHITE SNAKE/ BLACK LIZARD",
    "Women's Born Stowaway II Lago Studded Suede",
    "Michael Antonio Women's Mackenna Boot Black",
    "Men's Asics Gel-Blur33™ TR Mid Black/Silver",
    "Women's Drew Orchid Brown Nubuck",
    "CAROLINNA ESPINOSA 'Sylvie' Pump BLACK LEATHER",
    "Women's Sperry Top-Sider Rain Cloud Black",
    "Women's Clarks Seymour Dive Black",
    "Global Elements Jeweled Shoulder Bag Brown Brown",
    "Shimano SH-WR81 Road Cycling Shoes - 3-Hole For Women White",
    "Cole Haan 'Cassidy' Bootie BLACK NUBUCK",
    "Merrell Chameleon II Sandals - Convertible For Women ",
    "Men's Pikolinos Wellfeet Slip On 5481 Black",
    "Men's Dr Martens Holkham NS 5 Tie Hiker Gaucho Volcano",
    "Asolo Master Gore-Tex® Multi-Sport Boots - Waterproof For Women ",
    "Women's Earthies Tambolini Pink Brown",
    "Women's Seychelles Aquamarine Black",
    "Frye Women's Emma Woven Ballet Dk Brown Leather Dk Brown Leather",
    "Coconuts Women's Winchester Boot Saddle",
    "Anne Klein Juturna Black Leather /Gold",
    "Women's OTBT Bedford Soft Grey",
    "Lucky Brand 367 Vintage Boot 32\" in Nugget Nugget",
    "Sanuk Motorboat Brindle",
    "Nina 'Mirrin' Sandal BLACK",
    "Kipling G Creativity Pouch Orchid Stripe Multi",
    "Merrell Sway Leather Bracken",
    "UGG® Australia 'Lonna' Flat Women NIGHT",
    "Women's Jessica Simpson Waleo Brown",
    "Volatile Women's Swan Sandal Brown-Crocodile",
    "Merrell Chameleon II Shoes - Waterproof Gore-Tex® XCR® For Women ",
    "AK Anne Klein Dianthe Pumps - Buckle Strap For Women ",
    "Vince Camuto 'Billy' Satchel FUSION CORAL COMBO",
    "Romika® 'Fiona 01' Bootie BLACK"
  )
  val sc = sparkContext("WordDistribution")
  // You can make an RDD out of in-memory iterators or
  // from disk or
  // from a stream like a KafkaQueue
  val source = sc.makeRDD(lines)
  val words: RDD[String] = source
    .flatMap(line => line.split(" "))
    .filter(word => word.length > 2)
    .map(_.trim)
    .cache() // execute this transformation only once and cache it for future re-use starting here.

  // This is when the previous transformation is actually 'realized'.
  // Either during a collect() or save(), spark constructs a DAG and really
  // starts to executes your code.
  // Finally, we would've liked a Set[String] here to be more explicit
  // that we are dealing with distinct elements. But, because the collect API is generic,
  // we can't have it that way.
  val distinctWords: Array[String] = words.distinct().collect()
  println(s"Number of Distinct words: ${distinctWords.length}")

  // This is your typical word count, can run on really large datasets
  // across multiple nodes
  // Spark understands we want to group by key and get all values together as a sequence against that key.
  // So, it doesn't bother to duplicate the key information as part of the value list.
  // Also, as of Spark 0.9.*, groupBy operations return a RDD[K, Seq[V]]
  // This is a problem for large datasets, because, the Seq[V] is an in-memory structure, 
  // so it can't hold arbitrarily large sets.
  // This is different from the traditional hadoop's API, where the reducer gets a key and an Iterable of values, 
  // which allows Hadoop's reducers to process really large value streams. 
  val freqCount: RDD[(String, Int)] = words.map(word => (word, 1))
    .groupByKey()
    .map(kv => (kv._1, kv._2.sum))

  implicit val freqOrdering: Ordering[(String, Int)] = {
    Ordering.by{ t: (String, Int) => t._2 }
  }

  // Collecting the freq count as an in-memory map.
  // The transformations that's done to get freqCounts uses a cached version of the previous
  // transformation resulting in words.
  val collectedResult: Array[(String, Int)] = freqCount.top(10)(freqOrdering)
  println(s"Collected Result: ${collectedResult.length}")
  val outputLines = collectedResult.mkString("\n")
  println(s"Top-N words:\n${outputLines}")
}
