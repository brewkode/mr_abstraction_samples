package mapreduce.spike.scalding

import com.twitter.scalding._

class WordDistributionJob(args: Args) extends Job(args) {
  val input = args("input")
  val output = args("output")
  val K = 3

  def tokenize(line: String) = line.split(" ").map(_.trim).filterNot(_.isEmpty)
  def stopWord(word: String) = word.length <= 2

  TextLine(input)
  .name("WordDistribution Scalding")
  .flatMapTo('line -> 'word)(tokenize)
  .filter('line)(!stopWord)
  .map('word -> 'count)(1) // Creates a count field for every record(containing only the word so far).
  .groupBy('word)(_.sum('count, 'frequency)) // this is beautiful, it does a group by along with doing a map-side aggregation.
  .groupAll(_.sortedReverseTake(('frequency, 'word), K))
  .project('word, 'frequency)
  .write(Tsv(output, ('word, 'frequency)))

}
