package com._51doit.spark.year_day01


  case class MyMessage(event_type:Int,page:String,time:String,openid:String,goods:Array[Mygoods],pay_status:String,
                       channel:String,total_money:Double,longitude:Double,latitude:Double,province:String	,city:String,district:String)

  case class Mygoods(money : Int,pid:String	,cid:String,coverUrl:String,title:String	)
    /*
    局部json数组:goods
money : Int    	//商品价格
pid:String		  //商品id
cid:String		  //商品的分类 1：图书  2：服装   3：家具   4：手机
coverUrl:String	//服务url
title:String  	//商品的名称

event_type:Int		//事件类型 1：浏览    2：关注   3：添加购物车  4：结算
page:String  		//访问的页面
time:String 		//时间
openid:String		//用户id

全局的字段 外加goods
pay_status:String  	//支付状态	1: 成功		0：失败
channel:String		//支付方式
total_money:Double	//合计金额
longitude:Double	//经度
latitude:Double		//纬度
province:String 	//省
city:String 		 //市
district:String 	//区
     */


