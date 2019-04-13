package com

import com.atguigu.PAUTO.prop

package object atguigu {

  // 获取jdbc相关参数
  val driver = prop.getProperty("jdbcDriver")
  val jdbcUrl = prop.getProperty("jdbcUrl")
  val jdbcUser = prop.getProperty("jdbcUser")
  val jdbcPassword = prop.getProperty("jdbcPassword")
  //kafka相关参数
  val brokers = prop.getProperty("brokers")
  val topic = prop.getProperty("topic")
  // 设置批处理间隔
  val processingInterval = prop.getProperty("processingInterval").toLong

  val checkPoint = prop.getProperty("checkPoint")
}
