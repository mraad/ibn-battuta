package com.esri.battuta.dbf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._

/**
  */
case class DBFRDD(@transient sc: SparkContext, location: String, schema: StructType)
  extends RDD[Row](sc, Nil) with Logging {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    new Iterator[Row] {

      val part = split.asInstanceOf[DBFPartition]
      context.addTaskCompletionListener(context => close())

      val path = new Path(location)
      val conf = if (sc == null) new Configuration() else sc.hadoopConfiguration
      val fs = FileSystem.get(path.toUri, conf)
      val dataInputStream = fs.open(path)
      val dbfReader = new DBFReader(dataInputStream)

      var hasMore = DBFType.END != dbfReader.nextDataType()

      override def hasNext: Boolean = {
        hasMore
      }

      override def next(): Row = {
        // TODO - Handle passed schema
        val row = Row.fromSeq(dbfReader.readValues())
        hasMore = DBFType.END != dbfReader.nextDataType()
        row
      }

      def close() = {
        if (dataInputStream != null) {
          dataInputStream.close()
        }
      }
    }

  }

  override protected def getPartitions: Array[Partition] = {
    Array(DBFPartition(0))
  }
}

private[this] case class DBFPartition(idx: Int) extends Partition {
  override def index: Int = idx
}
