import java.util
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.Level
import org.apache.log4j.Logger
/**
 * Created by Administrator on 2017/6/22 0022.
 */
object KafkaSparkDemoMain {
  def main(args: Array[String]): Unit = {
    var batchSeconds = 5

    if (args.length > 0) {
      batchSeconds = args(0).toInt
    }


    // conf
    val sparkConf = new SparkConf().setAppName("ga-monitor")
    sparkConf.setMaster("local")

    // init streaming context 每5秒作为一个批次
    val ssc = new StreamingContext(sparkConf, Seconds(batchSeconds))

    // 获取配置文件
    //val topics=Array("ga_nginx_bi_yunqihome","ga_nginx_bi_tools")
    //    val topic="ga_nginx_bi_yunqihome"

    //         println( t )
    val kafkaDStream = createKafkaDStream(ssc,"test")

    kafkaDStream.foreachRDD(foreachFunc = rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val list = new util.ArrayList[String]()
        partitionOfRecords.foreach(item => {
          list.add(item._2);
          //            println(t);
          println(item._1+"-->"+item._2);
        })

      })
    })

    println("over")

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 创建kafkaDstream
   * @param ssc streaming context
   * @param configMap 配置map
   * @return dstream
   */
  def createKafkaDStream(ssc: StreamingContext,topic_str: String): InputDStream[(String, String)] = {
    // 设置kafka信息
    val topic = Set(topic_str)
    //val brokers = "bjc-1:9092,bjc-2:9092,bjc-3:9092,bjc-4:9092,bjc-5:9092"
    //    val brokers = "mq-55:19092,mq-57:19092,mq-58:19092,mq-126:19092"
    val brokers = "192.168.6.234:9092,192.168.6.233:9092,192.168.6.237:9092"
    val groupId = "ga-monitor"
    val kafkaParams = Map[String, String](
      "group.id" -> groupId,
      "metadata.broker.list" -> brokers,
      "refresh.leader.backoff.ms" -> "60000", // 允许kafka挂60秒
      "serializer.class" -> "kafka.serializer.StringEncoder")
    // 创建DStream
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic)
    kafkaDStream
  }
}
