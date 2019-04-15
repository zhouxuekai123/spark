package fuxiday01;

import java.util.ArrayList;
import java.util.List;


public class MyList2 {
    //定义一个私有变量,用来接收传进来的参数
    private List<String> oldList;

    //构造方法,方便new对象的时候把原有的集合传进来
    public MyList2(List<String> strList) {
        this.oldList = strList;
    }

    //定义一个方法,参数是一个接口
    public List<String> map(MMapFunction mf) {
        //定义一个集合,用来装新的数据
        ArrayList<String> tmp = new ArrayList<>();
        //遍历参数传进来的集合,对每个元素做操作
        for (String str : oldList) {
            //用接口调用方法,对sttr做处理
            String newStr = mf.transform(str);
            //元素添加到新的集合中
            tmp.add(newStr);
        }
        //返回新的集合
        return tmp;
    }

}
