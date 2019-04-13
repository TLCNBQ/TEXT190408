package com.atguigu

import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.City
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB}
import scalikejdbc._


/**
  * Paid abnormal user timely operation
  * 支付异常用户及时操作
  */
object PAUTO {

  // 从properties文件里获取各种参数
  val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("PAUTO.properties"))

  // 使用静态ip资源库
  val ipdb = new City(this.getClass.getClassLoader.getResource("ipipfree.ipdb").getPath)


  def main(args: Array[String]): Unit = {
    //设置jdbc
    Class.forName(driver)

    //设置连接池
    ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)

    // 通过getOrCreate方式可以实现从Driver端失败恢复
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkPoint, () => getSSC(checkPoint))


    // 启动流计算
    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * SparkConf
    * StreamingContext
    * kafkaParams: Map[String, String]
    * 获取Dstream streaming-》转换结构kafka streaming  ->a配置 b偏移量（从数据库读取）  c高阶函数（提取 topic  message）
    *
    *
    * @param checkPoint
    * @return
    */
  def getSSC(checkPoint: String): StreamingContext = {

    //SparkConf优雅关闭
    val sc: SparkConf = new SparkConf().set("spark.streaming.stopGracefullyOnShutdown", "true").setAppName(this.getClass.getSimpleName)
    //StreamingContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(processingInterval))

    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest"
    )

    //    1.配置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    //    2.偏移量（从数据库读取）
    // 获取offset
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
      sql"select topic, part_id, offset from unpayment_topic_offset"
        .map { r =>
          TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
        }.list.apply().toMap
    }

    //    3.高阶函数（提取 topic  message）
    val messageHandler: MessageAndMetadata[String, String] => (String, String) = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

    /**
      * 获取Dstream streaming-》转换结构kafka streaming  ->1.配置 2.偏移量（从数据库读取）  3.高阶函数（提取 topic  message）
      */
    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    //业物逻辑
    kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.filter { msg =>
      //过滤非完成订单的数据，且验证数据合法性
      filterCompleteOrderData(msg)
    }.map { msg =>
      //数据格式转换
      dataFormatConversion(msg)
    }.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(processingInterval * 4), Seconds(processingInterval * 2))
      .filter { state =>
        //过滤用户——订单异常
        filterUsersUnOrderBehavior(state)
      }.foreachRDD(rdd =>
      //开启事务
      foreachRDDLocalTx(rdd, offsetRanges))

    ssc

  }

  /**
    * 过滤非完成订单的数据，且验证数据合法性
    *
    * @param msg
    * @return
    */
  def filterCompleteOrderData(msg: (String, String)) = {
    val fields = msg._2.split("\t")
    // 切分后长度不为17，代表数据不合法
    if (fields.length == 17) {
      val eventType = msg._2.split("\t")(15)
      // 保留进入定单页
      "enterOrderPage".equals(eventType)
    } else {
      false
    }
  }

  /**
    *
    * 数据格式转换
    * （（uid,eventKey）,1）
    *
    * @param msg
    * @return
    */
  def dataFormatConversion(msg: (String, String)) = {
    val strings: Array[String] = msg._2.split("\t")
    val str: String = strings(0)
    (str, 1)
  }

  /**
    * 过滤用户——订单异常
    * 数量3次
    *
    * @param state
    * @return
    */
  def filterUsersUnOrderBehavior(state: (String, Int)) = {
    //获取用户id
    val uid: String = state._1
    //获取进入订单页的次数
    val count: Int = state._2
    //
    if (count >= 3) {
      val ints: List[Int] = DB.readOnly(implicit session => {
        sql"select id from vip_user where uid=${uid}"
          .map(r => {
            r.get[Int](1)
          }).list().apply()
      })
      if (ints.isEmpty) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  /**
    * foreachRDDLocalTx
    * 开启事务
    *
    * @param rdd
    * @param offsetRanges
    */
  def foreachRDDLocalTx(rdd: RDD[(String, Int)], offsetRanges: Array[OffsetRange]) = {
    rdd.foreachPartition(partition => {
      //开启事务
      DB.readOnly {
        implicit session =>
          partition.foreach(msg => {

            val uid: String = msg._1
            val value: Int = msg._2
            //println((uid, value))

            sql"""replace into unpayment_record(uid) values ${uid}""".executeUpdate().apply()
          })
          for (o <- offsetRanges) {
            //println(o.untilOffset,o.fromOffset,o.partition,o.topic)
            sql"""update unpayment_topic_offset set office = ${o.untilOffset} where topic = ${o.topic} and part_id = ${o.partition}""".update().apply()
          }
      }
    })

  }
}

