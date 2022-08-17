package com.scalademo;

public class valueTransform {
    String str = new String("good");
    char[] ch={'t','e','s','t'};
    public static void main(String[] args) {

        Data data = new Data();
        data.m=10;
        data.n=20;



        valueTransform ex = new valueTransform();
        ex.change(ex.str, ex.ch);
        System.out.println(ex.str);//good
        System.out.println(ex.ch);//best
    }
    public void change(String str,char ch[]){
        str="test ok";
        ch[0]='b';
    }

    public void swap(Data data){
        int temp=data.m;
        data.m = data.n;
        data.n = temp;
    }

    public void first(){
        int i=5;
        Value v=new Value();
        v.i=25;
        second(v,i);
        System.out.println(v.i);
    }

    public void second(Value v,int i){
        i=0;
        v.i=20;
        Value val=new Value();
        v=val;
        System.out.println(v.i+" "+i);

    }


}

class Data{
    int m;
    int n;
}

class Value{
    int i=15;
}
