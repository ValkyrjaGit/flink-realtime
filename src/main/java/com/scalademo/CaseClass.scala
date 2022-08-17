package com.scalademo

object CaseClass {
  def main(args: Array[String]): Unit = {
       val zhangsan = Person("zhangsan",20)
        println(zhangsan.toString)
  }
  case class Person(name:String,age:Int)
  trait Sex
  case object Male extends Sex
  case object Female extends Sex

}
