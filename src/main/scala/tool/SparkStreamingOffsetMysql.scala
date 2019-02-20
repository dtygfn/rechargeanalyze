package tool

import conf.ConfigurationManager
import constants.Constants
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

/**
  * 将偏移量保存到mysql中
  */
object SparkStreamingOffsetMysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkStreamingOffsetMysql")
      .setMaster("local[*]")
     val ssc = new StreamingContext(conf,Seconds(3))

    // 一系列基本的配置
    val groupid = ConfigurationManager.getProperty(Constants.GROUPID)
    val brokerList = ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST)
    val topic = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
    val topics = Set(topic)
    // 可能有多个topic
    val kafkas = Map(
      Constants.KAFKA_METADATA_BROKER_LIST->brokerList,
      Constants.GROUPID->brokerList,
      "auto.offset.reset"->kafka.api.OffsetRequest.SmallestTimeString // 设置数据的读取方式
    )

    // 加载配置
    DBs.setup()
    //这一块我们就不需要在进行查询ZK中的offset的了，直接查询MySQL中的offset数据

    val fromdbOffset :Map[TopicAndPartition,Long] =
      DB.readOnly{
        implicit session =>
          //查询每个分组下面的所有消息
          SQL(s"select * from offsets where groupId ='${groupid}'")
            //查询出来后，将MySQL中的数据赋值给这个元组
            .map(m=>(TopicAndPartition(
            m.string("topic"),m.int("partitions")),m.long("untilOffset")))
            .toList().apply()
      }.toMap //最后要toMap一下，因为前面的返回值已经给定
    // 创建一个InputDStream，然后根据offset读取数据
    var kafkaStream :InputDStream[(String,String)] = null
    //从MySQL中获取数据，进行判断
    if(fromdbOffset.size == 0){
      //如果程序第一次启动
      kafkaStream = KafkaUtils.
        createDirectStream[String,String,StringDecoder,StringDecoder](
        ssc,kafkas,topics)

    }else{
      //  如果程序不是第一次启动
      //  首先获取Topic和partition、offset
      var checkOffset = Map[TopicAndPartition,Long]()
      // 加载kafka的配置
      val kafkaCluster = new KafkaCluster(kafkas)
      //  首先获取Kafka中的所有Topic partition offset
      val earliesOffsets: Either[Err,
        Map[TopicAndPartition, KafkaCluster.LeaderOffset]] =
        kafkaCluster.getEarliestLeaderOffsets(fromdbOffset.keySet)
      //  然后开始进行比较大小，用MySQL中的offset和kafka的offset进行比较
        if(earliesOffsets.isRight){
          //取到我们需要的Map
          val topicAndPartitionOffset:
            Map[TopicAndPartition, KafkaCluster.LeaderOffset] =
            earliesOffsets.right.get
          // 来个直接进行比较大小
          checckOffset = fromdbOffset.map(owner=>{
            //取我们kafka汇总的offset
            val topicOffset = topicAndPartitionOffset.get(owner._1).get.offset
            //进行比较  不允许重复消费 取最大的
            if(owner._2 >topicOffset){
              owner
            }else{
              (owner._1,topicOffset)
            }
          })
        }

      // 不是第一次启动的话，按照之前的偏移量
      //不是第一次启动的话，按照之前的偏移量继续读取数据
      val messageHandler = (mmd:MessageAndMetadata[String,String])=>{
        (mmd.key(),mmd.message())
      }
      kafkaStream = KafkaUtils.
        createDirectStream[String,String,
        StringDecoder,StringDecoder,
        (String,String)](ssc,kafkas,checckOffset,messageHandler)
    }

    //开始处理数据流，跟咱们之前的ZK那一块一样了
    kafkaStream.foreachRDD(kafkaRDD=>{
      //首先将获取的数据转换 获取offset  后面更新的时候使用
      val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      val lines = kafkaRDD.map(_._2)
      lines.foreach(println)

      //更新偏移量
      DB.localTx{
        implicit session =>
          //取到所有的topic  partition offset
          for(os<-offsetRanges){
            //            // 通过SQL语句进行更新每次提交的偏移量数据
            //            SQL("UPDATE offsets SET groupId=?,topic=?,partitions=?,untilOffset=?")
            //              .bind(groupid,os.topic,os.partition,os.untilOffset).update().apply()
            SQL("replace into " +
              "offsets(groupId,topic,partitions,untilOffset) values(?,?,?,?)")
              .bind(groupid,os.topic,os.partition,os.untilOffset)
              .update().apply()
          }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
