package com.poc.statement.ingest

import java.util.Date

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.scalacheck._

/**
  * Created by tmatvienko on 12/9/15.
  */

case class Statement(account_id: Long, timestamp: Date, account_type: String,
                     amount: Float, merchant_id: Long, merchant_type: String)

object Generator extends App {
  private val cassandraHosts = "spark.cassandra.connection.host"

  private val cassandraUser = "spark.cassandra.auth.username"
  private val cassandraPass = "spark.cassandra.auth.password"

  val keyspaceName = "laundering_detection"
  val tableName = "statements"

  private val appConf = ConfigFactory.load()

  def cassandraSparkConf():SparkConf = {
    new SparkConf()
      .set(cassandraHosts, appConf.getString(cassandraHosts))
      .set(cassandraUser, appConf.getString(cassandraUser))
      .set(cassandraPass, appConf.getString(cassandraPass))
  }

  val conf = cassandraSparkConf().setAppName("statements-generator")
  val sc = new SparkContext(conf)

  var seq = Seq[Statement]()

  val accType = Gen.oneOf("checcking", "savings", "credit", "personal")
  val merchType = Gen.oneOf("retail", "service", "fuel", "travel")
  val idGen = Gen.choose(1000000l, 9999999l)
  val amountGen = Gen.choose(0f, 999999999f)

  (1 to 100).toStream.foreach(i => seq = Statement(idGen.sample.get, new Date(),
    accType.sample.get, amountGen.sample.get, idGen.sample.get, merchType.sample.get) +: seq)
  sc.parallelize(seq).saveToCassandra(keyspaceName, tableName)

}
