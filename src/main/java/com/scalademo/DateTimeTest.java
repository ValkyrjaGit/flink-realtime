package com.scalademo;

public class DateTimeTest {
    public static void main(String[] args) {
        Person man = new Man();
        man.eat();
        man.age=25;
        System.out.println(((Man) man).isSmoking);

    }
}

class Person {
    String name;
    int age;

    public void eat() {
        System.out.println("人，吃饭");
    }

    public void walk() {
        System.out.println("人，走路");
    }
}

class Woman extends Person {

    boolean isBeauty;

    public void goShopping() {
        System.out.println("女人喜欢购物");
    }

    public void eat() {
        System.out.println("女人少吃，为了减肥。");
    }

    public void walk() {
        System.out.println("女人，窈窕的走路。");
    }
}

class Man extends Person {

    boolean isSmoking;

    public void earnMoney() {
        System.out.println("男人负责工作养家");
    }

    public void eat() {
        System.out.println("男人多吃肉，长肌肉");
    }

    public void walk() {
        System.out.println("男人霸气的走路");
    }
}
