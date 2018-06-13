package cn.sheep.streaming

import cn.sheep.utils.{ConfParser, JPS, OffsetManager, Tools}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Sheep.Old @ 64341393
  * Created 2018/3/25
  */
object CMCCAppCore {

    Logger.getLogger("org").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf()
        // 设置参数
        sparkConf.setAppName("CMCC数据监控平台")
        sparkConf.setMaster("local[*]")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.set("spark.rdd.compress", "true")
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5000")
        val ssc = new StreamingContext(sparkConf, Seconds(1))


        // 使用广播变量广播省份编码映射关系
        val provinceDict = ssc.sparkContext.textFile(ConfParser.pcode2NamePath).map(line => {
            val split = line.split("\t")
            (split(0), split(1))
        }).collect().toMap
        val broadcast = ssc.sparkContext.broadcast(provinceDict)

        // 从kafka里面吧数据获取下来

        // 指定要从哪个主题下拉去数据
        val topics = Array(ConfParser.config.getString("cmcc.kafka.topics"))

        val groupId = ConfParser.config.getString("cmcc.kafka.group")
        // 指定kafka相关参数
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> ConfParser.config.getString("cmcc.kafka.brokers"),
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupId,
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )


        // 如果程序是第一次启动就应该从最早的偏移量开始消费数据
        // 如果程序非第一次启动的话，应该根据数据库中存储的偏移量接着往后消费，这个时候就需要指定偏移量进行消费
        // 查询数据库获取数据库中是否存储有偏移量信息
        val dbOffset: Map[TopicPartition, Long] = OffsetManager(topics, groupId)

        val stream: InputDStream[ConsumerRecord[String, String]] = if (dbOffset.size == 0) {
            // 通过直连的方式从kafka获取数据
            KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
            )
        } else {

            // 通过直连的方式从kafka获取数据
            KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Assign[String, String](dbOffset.keySet, kafkaParams, dbOffset)
            )
        }


        stream.foreachRDD(rdd => {
            // 如果该批次rdd为空了
            if (!rdd.isEmpty()) {

                // 获取一手的偏移量
                val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges


                // 开始业务指标的计算
                // 整理数据格式 - 数据大清洗 ETL -> (抽取, 转换, 加载)
                val baseRDD = rdd
                  .map(crd => JSON.parseObject(crd.value())) // 将日志数据拉出来，转成JSONObject
                  .filter(obj => obj.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
                  .map(obj => {
                      // 获取和指标相关的数据项
                      val brst = obj.getString("bussinessRst")
                      val fee = obj.getDouble("chargefee")


                      // 计算该次充值所耗时长
                      val reqId = obj.getString("requestId")
                      val rcnTime = obj.getString("receiveNotifyTime")

                      // （是否充值成功，充值成功时的金额，充值消耗的时长）
                      val succAndFeeAndShiCha: (Int, Double, Long) = if (brst.equals("0000")) {
                          val costTime = Tools.caluCostTime(reqId.substring(0, 17), rcnTime)
                          (1, fee, costTime)
                      } else (0, 0, 0)

                      val pCode = obj.getString("provinceCode")

                      // 数据当前日志
                      val day = reqId.substring(0, 8)
                      val hour = reqId.substring(8, 10)
                      val minutes = reqId.substring(10, 12)

                      // (省份, 是一个充值订单，是充值成功的订单吗，充值成功时的金额，充值成功时的时长)
                      (pCode, 1, succAndFeeAndShiCha._1, succAndFeeAndShiCha._2, succAndFeeAndShiCha._3, day, hour, minutes)
                  })



                // 计算业务概况指标（总订单量, 成功订单量, 充值成功金额, 充值总时长）
                baseRDD.map(t => (t._6, List[Double](t._2, t._3, t._4, t._5)))
                  .reduceByKey((list1, list2) => {
                      list1.zip(list2).map(t => t._1 + t._2)
                  }).foreachPartition(itr => {
                    val jedis = JPS.getJedis

                    itr.foreach(tp => {
                        jedis.hincrBy("A-"+tp._1, "succ", tp._2(1).toLong)
                        jedis.hincrByFloat("A-"+tp._1, "money", tp._2(2))
                        jedis.hincrBy("A-"+tp._1, "ttime", tp._2(3).toLong)
                        jedis.hincrBy("A-"+tp._1, "total", tp._2(0).toLong)
                    })

                    jedis.close()
                })


                // 统计实时充值办理趋势
                baseRDD.map(t => ((t._6, t._7), List[Double](t._2, t._3, t._4, t._5)))
                  .reduceByKey((list1, list2) => {
                      list1.zip(list2).map(t => t._1 + t._2)
                  }).foreachPartition(itr => {
                    val jedis = JPS.getJedis
                    itr.foreach(tp => {
                        jedis.hincrBy("A-"+tp._1._1, "t-"+tp._1._2, tp._2(0).toLong)
                        jedis.hincrBy("A-"++tp._1._1, "s-"+tp._1._2, tp._2(1).toLong)
                    })
                    jedis.close()
                })


                // 统计省份充值成功订单分布
                baseRDD.map(t => ((t._6, t._1), List[Double](t._2, t._3, t._4, t._5)))
                  .reduceByKey((list1, list2) => {
                      list1.zip(list2).map(t => t._1 + t._2)
                  }).foreachPartition(itr => {
                    val jedis = JPS.getJedis
                    itr.foreach(tp => {
                        val pname = broadcast.value.getOrElse(tp._1._2, tp._1._2)
                        jedis.hincrBy("B-"+tp._1._1, pname, tp._2(1).toLong)
                    })
                    jedis.close()
                })

                // 每分钟的充值订单及金额
                baseRDD.map(t => ((t._6, t._7, t._8), List[Double](t._2, t._3, t._4, t._5)))
                  .reduceByKey((list1, list2) => {
                      list1.zip(list2).map(t => t._1 + t._2)
                  }).foreachPartition(itr => {
                    val jedis = JPS.getJedis
                    itr.foreach(tp => {
                        val key = "C-"+tp._1._1 + tp._1._2 + tp._1._3

                        jedis.hincrBy(key, "succ", tp._2(1).toLong)
                        jedis.hincrByFloat(key, "money", tp._2(2))
                        // key的有效期
                        jedis.expire(key, 24 * 60 * 60)
                    })
                    jedis.close()
                })


                // 更新偏移量的
                OffsetManager(offsetRanges, groupId)
            }

        })

        // 启动程序
        ssc.start()
        ssc.awaitTermination()
    }


}
