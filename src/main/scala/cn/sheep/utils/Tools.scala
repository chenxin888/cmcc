package cn.sheep.utils

import org.apache.commons.lang3.time.FastDateFormat


/**
  * Sheep.Old @ 64341393
  * Created 2018/3/25
  */
object Tools {
    // FastDateFormat 线程安全
    private val fastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")

    // 求2个时间差的
    def caluCostTime(start: String, end: String): Long = {

        val endTime = fastDateFormat.parse(end).getTime
        val startTime = fastDateFormat.parse(start).getTime

        endTime - startTime
    }

}
