package fuxiday01;

import java.util.Arrays;
import java.util.List;

public class Demo {
    public static void main(String[] args) {
        String []arr={"tom","jim","hello"};
        List<String> list = Arrays.asList(arr);
        //new一个对象出来,把集合参数通过构造方法传进去
         MyList2 myList = new MyList2(list);

        List<String>result=myList.map(new MMapFunction() {
             @Override
             public String transform(String str) {
                 return str.toUpperCase();
             }
         });

        for(String s:result){
            System.out.println(s);
        }


    }

}


