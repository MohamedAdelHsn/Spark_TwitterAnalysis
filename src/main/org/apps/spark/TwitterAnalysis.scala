package org.apps.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.HashtagEntity
import org.apache.spark.SparkContext
import edu.stanford.nlp.util.CoreMap
import java.util.Properties
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.JavaConverters._

object TwitterAnalysis {

  def main(args: Array[String]): Unit = {
    System.setProperty("twitter4j.oauth.consumerKey", "<consumerKey>")
    System.setProperty("twitter4j.oauth.consumerSecret", "<consumerSecret>")
    System.setProperty("twitter4j.oauth.accessToken", "<accessToken>")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "<accessTokenSecret>")

    val sparkConf = new SparkConf().setAppName("twitter-streaming-analysis").setMaster("local[2]");
    val streamingContext = new StreamingContext(sparkConf, Seconds(1));
    streamingContext.sparkContext.setLogLevel("OFF");

    val tweets = TwitterUtils.createStream(streamingContext, None, Array("Egypt"))
    tweets.map(status => status.getText).map(tweet => (tweet, sentiment(tweet)))
      .foreachRDD(rdd => rdd.collect().foreach(tuple => println(" Sentiment => " + tuple._2 + " :-: TWEET => " + tuple._1)))

    streamingContext.start();
    streamingContext.awaitTermination();
  }

  def sentiment(tweets: String): String = {
    var mainSentiment = 0
    var longest = 0;
    val sentimentText = Array("Very Negative", "Negative", "Neutral", "Positive", "Very Positive")
    val props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
    new StanfordCoreNLP(props).process(tweets).get(classOf[CoreAnnotations.SentencesAnnotation]).asScala.foreach((sentence: CoreMap) => {
      val sentiment = RNNCoreAnnotations.getPredictedClass(sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree]));
      val partText = sentence.toString();
      if (partText.length() > longest) {
        mainSentiment = sentiment;
        longest = partText.length();
      }
    })
    sentimentText(mainSentiment)
  }
}