package cn.sheep.utils

import com.typesafe.config.ConfigFactory

/**
  * 解析配置文件
  * Sheep.Old @ 64341393
  * Created 2018/3/25
  */
object ConfParser {


    val config = ConfigFactory.load()


    val pcode2NamePath = config.getString("cmcc.pcode2name.path")


}
