package psu

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import scala.util.hashing.MurmurHash3
import scala.util.Random
import scala.collection.mutable.HashMap
import scala.collection.immutable.ListMap
import scala.math

case class UserBudget(user: User, budget: Float)

case class UserWord(user: User, word: Word)

object UserWord {
    implicit def orderingByUserWord[A <: UserWord] : Ordering[A] = {
       Ordering.by(fk => (fk.user, fk.word))
    }
}

class WordUserPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[UserWord]
      ((k.word % numPartitions) + numPartitions) % numPartitions
    }
}

object UserWordGroup {
    def group(pairs: RDD[(User, Word)], numPartitions: Int) : RDD[(UserWord,Int)]= {
        val uwParts = pairs.map{
            case (user, word) => {
                (UserWord(user, word), ((word % numPartitions) + numPartitions) % numPartitions)
            }
        }
        
        val uwCounts = pairs.map(p => (UserWord(p._1, p._2), 1)).reduceByKey(_ + _)

        val uCounts = uwCounts.map(uw => (uw._1.user, 1)).reduceByKey(_ + _)


        uwParts.repartitionAndSortWithinPartitions(new WordUserPartitioner(numPartitions))
    }

    def localCount(iter: Iterator[(UserWord, Word)], eps: Double, delta: Double, maxContrib: Int) : Iterator[(Word, Double)] = {
        val alpha = 5.0
        val l_param = 1.0 / eps
        val l_rho = (1 to maxContrib + 1).map(t => 1.0 / t + (1.0 / eps) * math.log(1.0 / (2.0 * (1.0 - math.pow((1.0 - delta),(1.0 / t)))))).max
        val gamma = l_rho + alpha * l_param
        val user_budget = 1.0

        val totals : HashMap[Int, Double] = new HashMap()

        var lastUser = 0 : Int
        var word = 0 : Int
        
        val gaps : HashMap[Int, Double] = new HashMap()

        while (iter.hasNext) {
            val record = iter.next
            val thisUser = record._1.user
            word = record._1.word

            val wordTotal = if (totals.contains(word)) totals(word) else 0.0
            if (wordTotal < gamma) {
                gaps(word) = gamma - wordTotal
            }

            if (thisUser != lastUser) {
                if (lastUser != 0) {
                    // process the user's words
                    var budget = user_budget
                    var sorted = ListMap(gaps.toSeq.sortBy(_._2):_*)
                    while (sorted.size > 0) {
                        val size = sorted.size
                        val (w, gap) = sorted.head
                        val cost = gap * size
                        if (cost <= budget) {
                            sorted.foreach(entry => {
                                val word = entry._1
                                val curTotal = if (totals.contains(word)) totals(word) else 0.0
                                totals(word) = curTotal + gap
                            })
                            sorted = sorted.tail.map(entry => (entry._1, entry._2 - gap))
                            budget = budget - cost
                        }
                        else {
                            sorted.foreach(entry => {
                                val word = entry._1
                                val curTotal = if (totals.contains(word)) totals(word) else 0.0
                                totals(word) = curTotal + (budget / size )
                            })
                            sorted = sorted.empty
                        }
                    }
                }
                lastUser = thisUser
                gaps.clear()
            }
        }

        totals.filter(_._2 > l_rho).toIterator
    }
}




object Top {
    def words(pairs: RDD[(WordLabel, Count)], n_records: Int) : Array[(WordLabel, Count)] = {
        pairs.takeOrdered(n_records)(Ordering[Int].reverse.on(_._2))
    }
}
object Histogram {
    def count(pairs: RDD[(User, Word)]) : RDD[(Word, Count)] = {
        pairs.map{
            case (user, word) => {
                (word, 1)
            }
        }.reduceByKey(_ + _)
    }
}

object UserPartitions {
    // report count of partitions that user will be in, if words are partitioned
    // in n_parts partitions
    def part(pairs: RDD[(User, Word)], n_parts : Int) : RDD[(User, Count)] = {
        pairs.map{
            case (user, word) => {
                // scala has signed modulus
                val partition = ((word % n_parts) + n_parts) % n_parts
                (user, partition)
            }
        }.distinct().map(p => (p._1, 1)).reduceByKey(_ + _)
    }
}


object Reservoir {
    def sample(pairs: RDD[(User, Word)], n_records: Int) : RDD[(User, Word)] = {
        pairs.groupByKey().flatMap{
            case (user, words) => {
                val wordList = words.toList
                if (wordList.length <= n_records) {
                    wordList.map(w => (user, w))
                } else {
                    Random.shuffle(wordList).take(n_records).map(w => (user, w))
                }
            }
        }
    }
}

object Murmurizer {
    def hash(pairs: RDD[(UserLabel, WordLabel)]) : RDD[(User, Word)] = {
        pairs.map{
            case (user, word) => {
                (MurmurHash3.stringHash(user), MurmurHash3.stringHash(word))
            }
        }
    }
}

object Dictionary {
    def lookup(histogram: RDD[(Word, Int)], words: RDD[WordLabel]) : RDD[(WordLabel, Int)] = {
        val wordLookup = words.distinct().map(word => (MurmurHash3.stringHash(word), word))
        val wordcounts = histogram.join(wordLookup)
        wordcounts.map(row => (row._2._2, row._2._1))
    }
}

object Tokenizer {

    def splitColumns(lines: RDD[String]) : RDD[(UserLabel, WordLabel)] = {
        lines.map(line => line.split(",")).filter(l => l.length > 2).map(l => (l(1), l(2)))
    }
    def tokenize(lines: RDD[String]) : RDD[(String, String)] = {
        splitColumns(lines).flatMap{case (user, text) => {
            text.replace("\"","").split(" ").map(word => (user, word))
        }}
    }
}

object Console {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext("local", "SetUnion", new SparkConf())
        try {

            val delta_0 = 10
            val n_parts = 2


            val input = sc.textFile("resources/data/clean_askreddit.csv").map(line => line.toLowerCase)
            val tokens = Tokenizer.tokenize(input)
            val hashes = Murmurizer.hash(tokens)
            val sampled = Reservoir.sample(hashes, delta_0)
            val hist = Histogram.count(sampled)
            //val dict = Dictionary.lookup(hist, tokens.map(_._2))
            //val top = Top.words(dict, 10)

            //val parts = UserPartitions.part(sampled, n_parts)
            //val parts_count = parts.map(p => (p._2, 1)).reduceByKey(_ + _)

            val groups = UserWordGroup.group(sampled, n_parts)

            val groupsCounted = groups.mapPartitions(part => UserWordGroup.localCount(part, 4.0, 10E-8, delta_0))
            
            //groupsCounted.saveAsTextFile("resources/streams/foo")

            val ngrams = groupsCounted.count()

            //Loader.writeCsv(parts_count, "wc.csv")

            println("-=-=-=-=-=-=-=-=-=-==-")
            println(s"Counted $ngrams ngrams")

            //top.foreach(println)
            //println(hist.count())
            //dict.take(10).foreach(println)
            //parts_count.foreach(println)
            println("-=-=-=-=-=-=-=-=-=-==-")
        } finally {
            sc.stop()
        }
    }
}