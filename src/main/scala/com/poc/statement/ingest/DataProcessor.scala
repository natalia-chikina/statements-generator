package com.poc.statement.ingest

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._


object DataProcessor extends App {

  private val cassandraHosts = "spark.cassandra.connection.host"

  private val cassandraUser = "spark.cassandra.auth.username"
  private val cassandraPass = "spark.cassandra.auth.password"

  val keyspaceName = "laundering_detection"
  val tableName = "statements"

  private val appConf = ConfigFactory.load()

  def cassandraSparkConf(): SparkConf = {
    new SparkConf()
      .set(cassandraHosts, appConf.getString(cassandraHosts))
      .set(cassandraUser, appConf.getString(cassandraUser))
      .set(cassandraPass, appConf.getString(cassandraPass))
  }

  val conf = cassandraSparkConf().setAppName("statements-generator")
  val sc = new SparkContext(conf)


  val suspiciousAccounts = sc.cassandraTable(keyspaceName, tableName)
    .groupBy(row => (row.getLong("account_id"), row.getLong("merchant_id"), row.getDate("timestamp")))
    .map(x => (x._1, x._2.foldLeft(0.0)((b, a) => b + a.getFloat("amount"))))
    .filter(_._2 > 10000)
  .map( x => Result(x._1._1, x._2) )

  case class Result(account_id: Long, amount: Double)

  suspiciousAccounts.saveAsCassandraTable(keyspaceName, "result", SomeColumns("account_id", "amount"))


  



  

}
