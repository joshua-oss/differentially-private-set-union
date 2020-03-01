package psu

import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.SparkContext._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import scala.util.hashing.MurmurHash3
import scala.util.Random


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
            val n_parts = 10


            val input = sc.textFile("resources/data/clean_askreddit.csv").map(line => line.toLowerCase)
            val tokens = Tokenizer.tokenize(input)
            val hashes = Murmurizer.hash(tokens)
            val sampled = Reservoir.sample(hashes, delta_0)
            val hist = Histogram.count(sampled)
            //val dict = Dictionary.lookup(hist, tokens.map(_._2))
            //val top = Top.words(dict, 10)

            //val parts = UserPartitions.part(sampled, n_parts)
            //val parts_count = parts.map(p => (p._2, 1)).reduceByKey(_ + _)

            val groups = sampled.map{
                case (user, word) => {
                    (UserWord(user, word), word)
                }
            }.repartitionAndSortWithinPartitions(new WordUserPartitioner(n_parts))
            
            groups.saveAsTextFile("resources/streams/foo")

            //Loader.writeCsv(parts_count, "wc.csv")

            //println("-=-=-=-=-=-=-=-=-=-==-")
            //top.foreach(println)
            //println(hist.count())
            //dict.take(10).foreach(println)
            //parts_count.foreach(println)
            //println("-=-=-=-=-=-=-=-=-=-==-")
        } finally {
            sc.stop()
        }
    }
}