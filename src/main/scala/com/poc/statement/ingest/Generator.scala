package com.poc.statement.ingest

import java.util.Date
import java.time._

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

  val initialDate = Date.from(LocalDateTime.of(2015, 11, 1, 1, 1, 1, 1).atZone(ZoneId.systemDefault()).toInstant).getTime
  val endDate = Date.from(LocalDateTime.of(2015, 12, 1, 1, 1, 1, 1).atZone(ZoneId.systemDefault()).toInstant).getTime

  implicit lazy val arbDate: Arbitrary[Date] = Arbitrary(Gen.choose(initialDate, endDate).map(new Date(_)))

  val accType = Gen.oneOf("checking", "savings", "credit", "personal")
  val timestampGen = Arbitrary.arbitrary[Date](arbDate)
  val merchType = Gen.oneOf("retail", "service", "fuel", "travel")
  val idGen = Gen.choose(1000000L, 9999999L)

  val amountGenNormal = Gen.choose(0f, 200f)
  val amountGenSuspicious = Gen.choose(10000f, 100000f)

  def generateSequence(accountNumber: Long, amountGen: Gen[Float]) = (1L to accountNumber).map(_ =>
    Statement(idGen.sample.get, timestampGen.sample.get,
      accType.sample.get, amountGen.sample.get, idGen.sample.get, merchType.sample.get))


  val percentage = 0.05
  val accountNumber = 100

  private val suspiciousAccountNumber = accountNumber * percentage.toLong
  
  val seq = List((suspiciousAccountNumber , amountGenSuspicious), (accountNumber - suspiciousAccountNumber , amountGenNormal))
    .flatMap(params => generateSequence(params._1, params._2))
  
  sc.parallelize(seq).saveToCassandra(keyspaceName, tableName)

}
