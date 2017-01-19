The goal of this programe is to write a standalone Spark streaming application with the following
characteristics. Spark Streaming 

Continuously read Twitter feed every 5 seconds.
Collect the feed in a sliding window of 60 seconds.

After each 5 seconds, calculate the number of retweets through all the retweets appearing
in that window. For this, first you have to filter the tweets, so that you are left only with
retweets. From a retweet, you can get the status of the original tweet
(http://twitter4j.org/javadoc/twitter4j/Status.html) and therefore also get its retweet
count. Note however, that since the free twitter feed gives you only 1% of the tweets, just
counting the number of retweets in a window is not a good method. What instead you must
do for this assignment is to get the retweet count from the retweet that appeared first in
the window (lets call that count value X) and get the retweet count from the retweet that
appeared last in the window (lets call that count value Y). Then, using Y – X + 1, you could
get the retweet count for the retweeted tweet.

That data that needs to be logged each 5 seconds has to be of the following format
<Secs>,<Lang>,<Langcode>,<TotalRetweetsInThatLang>,<IDOfTweet>,<MaxRetweetCount>,<MinRetweetCount>
,<RetweetCount>,<Text>

We can get the language used in the retweet by using the Apache Tika library (included
with the template code). It uses text analytics to figure out the language used. Since, tweets
contain short texts and informal language, many times Apache Tika will figure out wrong
language, for example in some cases, it will detect English as Norwegian. This kind of
analytics can be improved by using Machine learning, but for this assignment, it is enough
to use Apache Tika despite its inaccuracies.

The TotalRetweetsInThatLang has to be found out by combining all the retweet counts for
tweets in that language. IDOfTweet is the ID of the original tweet which has been
retweeted. MaxRetweetCount is found by looking at the retweet count through the
retweet that appeared last in the window (Hint: Reduce by Math.max function) while
MinRetweetCount is found by looking at the retweet count through the retweet that
appeared first in the window (Hint: Reduce by Math.min function). Adding 1 to the
difference between MaxRetweetCount and MinRetweetCount will give you the
RetweetCount for that tweet. Finally, Text is the text that appears in the tweet.

When you log data after every 5 seconds, you have to sort it using the
TotalRetweetsInThatLang value. This means that tweets for the language which has the
most combined retweet counts will appear first. Likewise, Tweets for other languages will
follow in descending order. For the tweets of the same language, the one with the most
retweet count should appear first, and others should follow in descending order.
