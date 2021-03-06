package HaoXin.LearFlink.WriteData

import java.text.DecimalFormat

import com.sun.deploy.util.StringUtils
import org.apache.commons.lang.RandomStringUtils
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class MyDataSource extends RichParallelSourceFunction[(String, String, Int, String, String, Int, String)] {
  var data_number = 0
  val format = new DecimalFormat("HX000000")

  override def run(ctx: SourceFunction.SourceContext[(String, String, Int, String, String, Int, String)]): Unit = {

    while (data_number < 999999) {
      println(data_number)
      val id = format.format(data_number)
      val name = GetName()
      val age = Random.nextInt(40) + 16
      val sex = RandomStringUtils.random(1, "男女")
      val city = GetAddress()
      val money = Random.nextInt(250) + 50
      val to_city = GetAddress()
      ctx.collect((id, name, age, sex, city, money, to_city))
      data_number += 1
    }



}

override def cancel (): Unit = data_number = 999999



def GetName (): String = {

  val all = "赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜戚谢邹喻柏水窦章云苏潘葛奚范彭郎鲁韦昌马苗凤花方俞任袁柳酆鲍史唐" +
  "费廉岑薛雷贺倪汤滕殷罗毕郝邬安常乐于时傅皮卞齐康伍余元卜顾孟平黄和穆萧尹姚邵湛汪祁毛禹狄米贝明臧计伏成戴谈宋茅庞熊纪舒屈项祝董" +
  "梁杜阮蓝闵席季麻强贾路娄危江童颜郭梅盛林刁钟徐邱骆高夏蔡田樊胡凌霍虞万支柯昝管卢莫经房裘缪干解应宗丁宣贲邓郁单杭洪包诸左石崔吉" +
  "钮龚程嵇邢滑裴陆荣翁荀羊於惠甄曲家封芮羿储靳汲邴糜松井段富巫乌焦巴弓牧隗山谷车侯宓蓬全郗班仰秋仲伊宫宁仇栾暴甘钭厉戎祖武符刘景" +
  "詹束龙叶幸司韶郜黎蓟薄印宿白怀蒲邰从鄂索咸籍赖卓蔺屠蒙池乔阴鬱胥能苍双闻莘党翟谭贡劳逄姬申扶堵冉宰郦雍卻璩桑桂濮牛寿通边扈燕冀" +
  "郏浦尚农温别庄晏柴瞿阎充慕连茹习宦艾鱼容向古易慎戈廖庾终暨居衡步都耿满弘匡国文寇广禄阙东欧殳沃利蔚越夔隆师巩厍聂晁勾敖融冷訾辛阚" +
  "那简饶空曾毋沙乜养鞠须丰巢关蒯相查后荆红游竺权逯盖益桓公万俟司马上官欧阳夏侯诸葛闻人东方赫连皇甫尉迟公羊澹台公冶宗政濮阳淳于单于" +
  "太叔申屠公孙仲孙轩辕令狐钟离宇文长孙慕容鲜于闾丘司徒司空丌官司寇仉督子车颛孙端木巫马公西漆雕乐正壤驷公良拓跋夹谷宰父谷梁晋楚闫法" +
  "汝鄢涂钦段干百里东郭南门呼延归海羊舌微生岳帅缑亢况郈有琴梁丘左丘东门西门商牟佘佴伯赏南宫墨哈谯笪年爱阳佟第五言福百家姓终"

  val tName = "风花雪月山树楼台一奕如娜大小淑书静香陌莫沫眸忆安简素未离央墨凉荼曦兮顾初旧落逆倾颜微锦诺鑫龙佳伟秀周迪峰"
  all

  val oneName = s"${
  RandomStringUtils.random (1, all)
}${
  RandomStringUtils.random (1, tName)
}${
  if (Random.nextInt (12)
  % 2 == 0) RandomStringUtils.random (1, tName) else ""
}"


  oneName
}

  def GetAddress (): String = {

  val cityList = List ("杭州", "宁波", "金华", "绍兴", "台州", "舟山", "应县", "大同", "太原", "丽江")

  cityList (Random.nextInt (5) )
}
}
