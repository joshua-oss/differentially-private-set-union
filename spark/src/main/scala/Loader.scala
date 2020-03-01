package psu

import scala.util.Random
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

object Config {
    def base = if (local) "resources/streams/" else "dbfs:/FileStore/tables/PSU/"
}


object Loader {
    def compact(fileName: String, destFileName: String): Unit =  {
        (new RawLocalFileSystem).delete(new Path(destFileName), true)
        val hadoopConfig = new Configuration()
        val hdfs = FileSystem.get(hadoopConfig)
        FileUtil.copyMerge(hdfs, new Path(fileName), hdfs, new Path(destFileName), false, hadoopConfig, null)
        (new RawLocalFileSystem).delete(new Path(fileName), true)
    }

    def writeCsv(rdd: RDD[(Int, Int)], fileName: String) : Unit = {
        val path = Config.base + fileName
        val path_tmp = path + "_tmp"
        (new RawLocalFileSystem).delete(new Path(path_tmp), true)
        val rows = rdd.map(r => Array(r._1, r._2).mkString(","))
        rows.saveAsTextFile(path_tmp)
        compact(path_tmp, path)
    }


}