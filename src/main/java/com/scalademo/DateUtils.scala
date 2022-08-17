package com.scalademo

import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {

   val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")

    def format(date:Date): String =sdf.format(date)

    def main(args: Array[String]): Unit = {
      println(DateUtils.format(new Date()))
    }


}
