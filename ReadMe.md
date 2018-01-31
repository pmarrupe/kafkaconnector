# Spark Connector for Twitter

1. This is used to stream data from twitter apis and make good use of it.

2. Primary purpose of this job is to get all the tweets, convert them to json and put it on `tweet.data` topic.

3. Secondly, it calculates the hashtag count in an interval batch and put them on `TweetHashTags1` topic.

4. The format of the data is something like this:

`1 #Google` where 1 is the number of times #google appeared in that batch interval.

5. To consume the data on TweetHashTags1 topic run the following command:

`bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic TweetHashTags1`

(p.s assuming you have kafka and zookeeper running locally)

