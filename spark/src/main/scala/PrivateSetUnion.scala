import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.util.hashing.MurmurHash3
import scala.util.Random


object Top {
    def words(pairs: RDD[(String, Int)], n_records: Int) : Array[(String, Int)] = {
        pairs.takeOrdered(n_records)(Ordering[Int].reverse.on(_._2))
    }
}
object Histogram {
    def count(pairs: RDD[(Int, Int)]) : RDD[(Int, Int)] = {
        pairs.map{
            case (user, word) => {
                (word, 1)
            }
        }.reduceByKey(_ + _)
    }
}

object Reservoir {
    def sample(pairs: RDD[(Int, Int)], n_records: Int) : RDD[(Int, Int)] = {
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
    def hash(pairs: RDD[(String, String)]) : RDD[(Int, Int)] = {
        pairs.map{
            case (user, word) => {
                (MurmurHash3.stringHash(user), MurmurHash3.stringHash(word))
            }
        }
    }
}

object Dictionary {
    def lookup(histogram: RDD[(Int, Int)], words: RDD[String]) : RDD[(String, Int)] = {
        val wordLookup = words.distinct().map(word => (MurmurHash3.stringHash(word), word))
        val wordcounts = histogram.join(wordLookup)
        wordcounts.map(row => (row._2._2, row._2._1))
    }
}

object Tokenizer {

    def splitColumns(lines: RDD[String]) : RDD[(String, String)] = {
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
            val input = sc.textFile("resources/data/clean_askreddit.csv").map(line => line.toLowerCase)
            val tokens = Tokenizer.tokenize(input)
            val hashes = Murmurizer.hash(tokens)
            val res = Reservoir.sample(hashes, 10)
            val hist = Histogram.count(res)
            val dict = Dictionary.lookup(hist, tokens.map(_._2))
            val top = Top.words(dict, 10)
            println("-=-=-=-=-=-=-=-=-=-==-")
            top.foreach(println)
            println(hist.count())
            dict.take(10).foreach(println)
            println("-=-=-=-=-=-=-=-=-=-==-")
        } finally {
            sc.stop()
        }
    }
}