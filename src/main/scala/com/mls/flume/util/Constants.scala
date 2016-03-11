package com.mls.flume.util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by zhangzhikuan on 16/3/11.
  */
object Constants {
  val TOPIC_SPLIT = "_"

  //时间模版
  private val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")

  def date2String(time: Long): String = {
    dateFormat.format(new Date(time))
  }
}
