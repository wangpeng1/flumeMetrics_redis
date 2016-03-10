package com.mls.flume

import java.net.InetAddress

import org.apache.flume.Context
import org.apache.flume.instrumentation.MonitorService
import org.apache.flume.instrumentation.util.JMXPollUtil
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

/**
  * Created by zhangzhikuan on 16/3/9.
  */
class RedisMonitorServer extends MonitorService {
  //打印日志
  val logger = LoggerFactory.getLogger(classOf[RedisMonitorServer])
  //机器主机名
  val hostName: String = {
    InetAddress.getLocalHost.getHostName
  }
  //redis客户端
  var redis: Jedis = _

  //待收集的选项
  val attributeList = List(
    //channel:成功写入channel且提交的事件总数量
    "EventPutSuccessCount",
    //sink成功读取的事件的总数量
    "EventTakeSuccessCount",
    //目前channel中事件的总数量
    //    "ChannelSize",

    //目前为止source已经接收到的事件总数量
    "EventReceivedCount",
    //source:成功写出到channel的事件总数量
    "EventAcceptedCount",

    //sink:成功写出到存储的事件总数量
    "EventDrainSuccessCount"
  )

  //保存历史记录的
  val valMap = collection.mutable.Map[String, Long]()


  //定时逻辑
  def doRun(time: Long): Unit = {
    try {
      //获取所有的信息
      val t: java.util.Map[String, java.util.Map[String, String]] = JMXPollUtil.getAllMBeans

      val it = t.keySet.iterator
      //开始遍历
      while (it.hasNext) {
        val component_key = it.next
        //组件也是一个map
        val attributeMap: java.util.Map[String, String] = t.get(component_key)

        //进行迭代
        val it$ = attributeMap.keySet().iterator()
        while (it$.hasNext) {
          val attribute_key = it$.next
          //属性,组件+属性=>value
          val flume_key = s"flume.${component_key}.${attribute_key}"
          val redis_key = s"${time}::${hostName}::${flume_key}"

          //开始往redis写数据
          try {
            if (attributeList.contains(attribute_key)) {
              //value值
              val newValue = attributeMap.get(attribute_key).toLong
              //获取原有的值
              val oldValue = valMap.getOrElse(flume_key, 0L)

              //发送给redis
              //时间戳key

              val time_num = newValue - oldValue
              redis.set(redis_key, time_num.toString, "NX", "EX", 900)

              //设置新值
              valMap.put(flume_key, newValue)

              logger.info(s"oldValue[${oldValue}],newValue[${newValue}]")

              logger.debug("增量[" + flume_key + "]:[" + time_num + "]")
            } else if ("ChannelSize".equals(attribute_key)) {
              //记录当前channel的大小
              redis.set(redis_key, attributeMap.get(attribute_key), "NX", "EX", 900)
            }

          } catch {
            case e: Exception => logger.warn(s"Metric:Component[${component_key}]-Attribute[${attribute_key}]收集失败", e);
          }
        }
      }
    } catch {
      case t: Throwable => logger.error("未知错误", t);
    }
  }

  //定时器
  val timer = new RecurringTimer(new SystemClock, 5 * 1000, doRun, "RedisMonitorServer")

  //重写
  override def start(): Unit = {

    //清零,其实如果可以做到持久化会不会好点呢
    //定时器重启
    valMap.clear()
    timer.start()
  }

  override def stop(): Unit = {
    //判断redis是否为空,如果不为空,则关闭
    if (redis != null) {
      redis.close()
    }

    //将保存的数据清理
    valMap.clear()

    //定时器关闭
    timer.stop(true)
  }

  override def configure(context: Context): Unit = {
    //创建redis客户端
    redis = new Jedis(context.getString("ip", "127.0.0.1"), context.getInteger("port", 6379))
    //选择redis的db
    redis.select(context.getInteger("db", 0))
  }
}