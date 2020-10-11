package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build() 
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    // create table avg with two columns (types text, float)
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);") 

    // make a connection to Kafka and read (key, value) pairs from it
    val sparkConf = new SparkConf().setAppName("AvgValue").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2)) // TODO come back to time
    ssc.checkpoint("checkpoint")

    // define Kafka params as Map of config parameters to their values
    val kafkaConf = Map(
		"metadata.broker.list" -> "localhost:9092",
		"zookeeper.connect" -> "localhost:2181",
		"group.id" -> "kafka-spark-streaming",
		"zookeeper.connection.timeout.ms" -> "1000")
    val setOfTopics = Set("avg")

	
    // We use receiver-less direct approach where spark periodically queries Kafka for latest offsets in each topic+partition
    // and defines offset ranges to process in each batch 
    // key Class, value class, key decoder class, value decoder class
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
	ssc, kafkaConf, setOfTopics)
    // pass in streaming context, map of kafka params, set of topic to consume
    val linesArr = kafkaStream.map(x => x._2.split(",")) // get lines and split 
    val pairs = linesArr.map(x => (x(0), x(1).toInt)) // taking from array so (0), (1)
	
    // measure the average value for each key in a stateful manner (i.e. only on keys that are available in last micro batch) 
    // keys are the count 
    // values are the avg

    def mappingFunc(key: String, value: Option[Int], state: State[(Int, Int)]): (String, Double) = {
	// if state exists, use that for current count and avg, otherwise initialise to 0.
	// store (sum, n) as state and then can compute avg from that
	val newVal = value.getOrElse(0) // get new value 
	val prevState = state.getOption.getOrElse((0,0))

	val newSum : Int = prevState._1 + newVal
	val newCount = prevState._2 +1

	val avg: Double = newSum.toDouble / newCount
	// println("key", key, ", avg", avg)
	state.update((newSum, newCount))
	(key, avg)

    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _)) 

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))
    ssc.checkpoint("./checkpoints")
    ssc.start()
    ssc.awaitTermination()
    session.close()
  }
}
