package com.scalademo

import scala.io.StdIn

object MatchDemo {
  def main(args: Array[String]): Unit = {
//      println("请输入一个词:")
//    val name: String =StdIn.readLine()
//    val result: String =name match{
//      case "hadoop" => "大数据分布式存储和计算框架"
//      case "zookeeper" => "大数据分布式协调服务框架"
//      case "spark" => "大数据分布式内存计算框架"
//      case _ => "未匹配"
//    }
//
//    println(result)


//    val a:Any="hadoop"
//    val result: String =a match {
//      case a:String =>"String"
//      case a:Int=>"Int"
//      case a:Double=>"Double"
//    }
//    println(result)

//    val a: Int =StdIn.readInt()
//
//    a match{
//      case a if a>=0 && a<=3 =>println("[0-3]")
//      case a if a>=4 && a<=8 =>println("[4-8]")
//      case _ =>
//    }


    val zhangsan: Any = Person("张三", 20)
    val order: Any = Order("001")

    order match{
      case Person(name,age)=>println(s"姓名：$name 年龄：$age")
      case Order(id)=> println(s"ID为：$id")
      case _ =>println("未匹配")
    }




  }
  case class Person(name:String,age:Int)
  case class Order(id:String)

  def dvi(a:Double, b:Double)={
    if(b!=0){
      Some(a/b)
    }else{
      None
    }
  }

}
