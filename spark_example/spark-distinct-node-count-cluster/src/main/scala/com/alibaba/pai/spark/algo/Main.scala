package com.alibaba.pai.spark.algo

import com.aliyun.odps.{Column, OdpsType, TableSchema}
import com.aliyun.odps.cupid.client.spark.api.{JobContext, SparkJob}
import com.aliyun.odps.cupid.client.spark.util.serializer.JavaSerializerInstance
import com.aliyun.odps.data.Record
import com.typesafe.config.Config
import org.apache.spark.odps.OdpsOps
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object Main extends SparkJob {

  var inTableName:String = _
  var outTableName:String = _
  var numPartitions:Int = _

  override def runJob(jobContext: JobContext, jobConf: Config): Array[Byte] = {
    inTableName = jobConf.getString("inputTableName")
    val fromNodeCol = jobConf.getString("fromNodeCol")
    val toNodeCol = jobConf.getString("toNodeCol")
    outTableName = jobConf.getString("outputTableName")
    numPartitions = jobConf.getInt("numPartitions")

    val ops = new OdpsOps(jobContext.sc())
    val odps = jobContext.odps()

    val inTable = odps.tables().get(inTableName)
    val fCol = inTable.getSchema.getColumn(fromNodeCol)
    val tCol = inTable.getSchema.getColumn(toNodeCol)
    if (fCol.getTypeInfo.getOdpsType != tCol.getTypeInfo.getOdpsType) {
      throw new Exception("The type of fromNodeCol and toNodeCol should be same.")
    }

    fCol.getTypeInfo.getOdpsType match {
      case OdpsType.BIGINT => doNodeCount[Long](ops, jobContext, fromNodeCol, toNodeCol)
      case OdpsType.STRING => doNodeCount[String](ops, jobContext, fromNodeCol, toNodeCol)
      case _ => throw new Exception("Invalid node column, only support bigint or string.")
    }

    JavaSerializerInstance.getInstance.serialize("OK").array()
  }



  private def doNodeCount[T: ClassTag](ops: OdpsOps, ctx: JobContext,
                                      fromNodeCol:String, toNodeCol:String): Unit = {

    val cols = ctx.sc.broadcast((fromNodeCol, toNodeCol))

    val edges: RDD[(T, T)] = ops.readTable[Tuple2[T, T]](
      ctx.odps.getDefaultProject, inTableName,
      (r: Record, s: TableSchema) =>
        (r.get(cols.value._1).asInstanceOf[T],
        r.get(cols.value._2).asInstanceOf[T]), this.numPartitions)

    val totNodes = edges.flatMap(x => List(x._1, x._2))
    val nodeCnt = totNodes.map(x => (x, 1)).reduceByKey{case (x, y) => x+y}
    // save to table
    ops.saveToTable[(T, Int)](ctx.odps.getDefaultProject, outTableName, nodeCnt,
      (v:(T, Int), r:Record, s:TableSchema) => {
        r.set(0, v._1)
        r.set(1, v._2.toLong)
      })
  }

}
