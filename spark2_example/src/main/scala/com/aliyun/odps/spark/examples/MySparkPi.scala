package com.aliyun.odps.spark.examples

import org.apache.spark._

import scala.math.random

/** Computes an approximation to pi */
object MySparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val sc = new SparkContext(conf)
    try {
      val slices = if (args.length > 0) args(0).split("=")(1).toInt else 2
      val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
      val count = sc.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y < 1) 1 else 0
      }.reduce(_ + _)
      println("Pi is roughly " + 4.0 * count / n)
    } finally {
      sc.stop()
    }
  }
}