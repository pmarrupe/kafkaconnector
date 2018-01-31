
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import com.google.common.base.Optional;
import java.util.*;

public class Application {

    public static void main(String[] args) throws InterruptedException {
        //StreamingExamples.setStreamingLogLevels();
        // Set logging level if log4j not configured (override by adding log4j.properties to classpath)

        ObjectMapper mapper = new ObjectMapper();
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

                //If the request fails, the producer can automatically retry,
                props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");



        String consumerKey = "XJF8d49R2h6n7g75CU2bpm2RR";
        String consumerSecret = "Bdj42CBVZyUXhfJHVEdbQrd4xwnB6eSqW8AVoHKfOB3URubc34";
        String accessToken = "956981239667396609-UQLh8KJ2PWFGBaEkq80pJroVWDK7TdQ";
        String accessTokenSecret = "dAqCdQVflLcmp7sSw0potcC9W7xQy07NePVdumnDJazrB";

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);


        SparkConf sparkConf=new SparkConf().setAppName("Tweets Android").setMaster("local[2]");
        JavaStreamingContext sc=new JavaStreamingContext(sparkConf,new Duration(5000));
        sc.checkpoint("/Users/pmarrupedhi/Downloads/sparkconnector/src/main/resources");

        // Create a data stream using streaming context and Twitter authorization
        final JavaReceiverInputDStream<Status> inputDStream = TwitterUtils.
                createStream(sc, new String[]{"#google"});

        JavaDStream<Tweet> tweets = inputDStream
                .filter(status -> isTweetEnglish(status))
                .map(status -> buildNewTweet(status));

        tweets.foreach(new Function<JavaRDD<Tweet>, Void>() {
            @Override
            public Void call(JavaRDD<Tweet> tweetJavaRDD) throws Exception {
                tweetJavaRDD.foreach(tweet -> {
                    Producer<String, String> producer = new KafkaProducer<String, String>(props);
                    String data = mapper.writeValueAsString(tweet);
                    producer.send(new ProducerRecord<String, String>("tweet.data", "tweet", data));
                });
                return null;
            }
        });

        JavaPairDStream<String, Long> hashtags = tweets.flatMapToPair(new PairFlatMapFunction<Tweet, String, Long>() {
            @Override
            public Iterable<Tuple2<String, Long>> call(Tweet o) throws Exception {
                String[] words = o.getText().split(" ");
                if(words.length>0) {
                    System.out.print(words);
                }
                ArrayList<Tuple2<String, Long>> hashtags = new ArrayList<Tuple2<String, Long>>();
                for (String word : words) {
                    if(word.startsWith("#")){
                        hashtags.add(new Tuple2<String, Long>(word, (long) 1));
                    }
                }
                return hashtags;
            }
        });

        JavaPairDStream<String, Long> hashtagsCount = hashtags.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
        @Override
            public Optional<Long> call(List<Long> in1, Optional<Long> in2){
            long newSum = in2.or((long) 0);
            for (long i : in1) {
                newSum += i;
            }
            if(newSum>1)
                System.out.print(newSum);
            return Optional.of(newSum);
        }
        });

        hashtagsCount.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {

            @Override
            public Void call(JavaPairRDD<String, Long> in) throws Exception {
                in = in.mapToPair(a -> a.swap()).sortByKey(true).mapToPair(a -> a.swap());
                List<Tuple2<String, Long>> topList = in.collect();
                String key = String.valueOf(in.count());

                for (Tuple2<String, Long> pair : topList) {
                    new KafkaProducer<String, String>(props).send(new ProducerRecord<>("TweetHashTags1", pair._2().toString(), pair._2()+" "+ pair._1()));
                    System.out.println(
                            String.format("%s (%s happiness)", pair._2(), pair._1()));
                }

                return null;
            }
        });

        // Triggers the sta
        sc.start();
        sc.awaitTermination();
    }

    private static Tweet buildNewTweet(Status status) {
        return new Tweet(status.getUser().getId(),
                status.getUser().getName(),
                status.getUser().getScreenName(),
                status.getUser().getMiniProfileImageURL(),
                replaceNewLines(status.getText()),
                status.getGeoLocation() == null ? null : status.getGeoLocation().getLatitude(),
                status.getGeoLocation() == null ? null : status.getGeoLocation().getLongitude(),
                status.getLang(),
                status.getSource(),
                0,
                new Date(),
                status.getRetweetCount(),
                status.getUser().getFriendsCount());
    }

    protected static boolean isTweetEnglish(Status status) {
      return "en".equals(status.getLang()) && "en".equals(status.getUser().getLang());
    }

    private static String replaceNewLines(String text) {
        return text.replace("\n", "");
    }

    private static boolean hasGeoLocation(Status status) {
        return status.getGeoLocation() != null;
    }
}
