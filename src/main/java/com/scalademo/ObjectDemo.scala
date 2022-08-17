package com.scalademo


object ObjectDemo {

  def main(args: Array[String]): Unit = {
//    val zhangsan = new Person("张三",20)
//    val man40 = new Person(age=40)
//    val customer = new Customer(Array("zhansan","beijing"))
//    println(customer.tel)

//    println(Dog.LEG_NUM)
//    Dog.printSpliter()
//    val zhangsan = Person("zhangsan",20)

val logger = new ConsoleLogger
    logger.log("this")


  }

     object Dog{
       val LEG_NUM=4

       def printSpliter(): Unit ={
         println("_" * 10)
       }

     }

     class Person(var name:String="",var age:Int=0){
      println("调用主构造器")
     }

    object Person{
      def apply(name:String,age:Int)=new Person(name,age)
    }

     class Customer(var name:String="",var address:String=""){
      var tel:String=_
      def this(arr:Array[String])={
        this()
        tel=arr(0)
      }
    }

    class CustomerService {
      def save(): Unit = {
        println(s"${CustomerService.SERVICE_NAME}:保存客户")
      }
    }

    // CustomerService的伴生对象
    object CustomerService {
      private val SERVICE_NAME = "CustomerService"
    }
 class Student extends Person

  trait Logger{
    def log(message:String)
  }

  class ConsoleLogger extends Logger{
    override def log(message: String): Unit = println("控制台日志:" + message)
  }

  trait LoggerDetail{
    def log(msg:String): Unit =println(msg)
  }





}
