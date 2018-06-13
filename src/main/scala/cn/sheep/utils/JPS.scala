package cn.sheep.utils

import com.typesafe.config.ConfigFactory
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Sheep.Old @ 64341393
  * Created 2018/3/25
  */
object JPS {

    private val load = ConfigFactory.load()

    private val genericObjectPoolConfig = new GenericObjectPoolConfig()
    genericObjectPoolConfig.setMaxIdle(10)
    genericObjectPoolConfig.setMaxTotal(200)

    private lazy val jedisPool = new JedisPool(genericObjectPoolConfig,
        load.getString("cmcc.redis.host"),
        load.getInt("cmcc.redis.port"),
        load.getInt("timeOut"),
        null,
        load.getInt("cmcc.redis.index")
    )


    def getJedis: Jedis = jedisPool.getResource

}
