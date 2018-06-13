package cn.sheep.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import scalikejdbc._
import scalikejdbc.config._

/**
  *
  * 从数据库中查询|更新偏移量信息
  * Sheep.Old @ 64341393
  * Created 2018/3/25
  */
object OffsetManager {

    // 加载配置文件
    DBs.setup()

    /**
      * 查询数据库获取偏移量信息
      * @return
      */
    def apply(topics: Array[String], groupId: String): Map[TopicPartition, Long] = {
        DB.readOnly{ implicit session =>
            SQL("select * from streaming_offset_29 where topic=? and groupId=?")
              .bind(topics.head, groupId)
              .map(rs => (new TopicPartition(rs.string("topic"), rs.int("partitionId")), rs.long("untilOffset")))
              .list().apply()
        }.toMap

    }


    // 更新偏移量
    def apply(offsetRanges: Array[OffsetRange], groupId: String) = {
        DB.localTx{ implicit session =>
            for (range <- offsetRanges ) {
                SQL("replace into streaming_offset_29 values(?,?,?,?)")
                  .bind(range.topic, range.partition, groupId, range.untilOffset)
                  .update().apply()
            }
        }
    }




}
