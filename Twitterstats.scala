import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern

object Twitterstats
{ 
	var firstTime = true
	var t0: Long = 0
	val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("twitterLog.txt"), "UTF-8"))
	
	// This function will be called periodically after each 5 seconds to log the output. 
	// Elements of a are of type (lang, totalRetweetsInThatLang, idOfOriginalTweet, text, maxRetweetCount, minRetweetCount)
	def write2Log(a: Array[(String, Long, Long, String, Long, Long)])
	{
		if (firstTime)
		{
			bw.write("Seconds,Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,MaxRetweetCount,MinRetweetCount,RetweetCount,Text\n")
			t0 = System.currentTimeMillis
			firstTime = false
		}
		else
		{
			val seconds = (System.currentTimeMillis - t0) / 1000
			
			if (seconds < 60)
			{
				println("Elapsed time = " + seconds + " seconds. Logging will be started after 60 seconds.")
				return
			}
			
			println("Logging the output to the log file\nElapsed time = " + seconds + " seconds\n-----------------------------------")
			
			for(i <-0 until a.size)
			{
				val langCode = a(i)._1
				val lang = getLangName(langCode)
				val totalRetweetsInThatLang = a(i)._2
				val id = a(i)._3
				val textStr = a(i)._4.replaceAll("\\r|\\n", " ")
				val maxRetweetCount = a(i)._5
				val minRetweetCount = a(i)._6
				val retweetCount = maxRetweetCount - minRetweetCount + 1
				
				bw.write("(" + seconds + ")," + lang + "," + langCode + "," + totalRetweetsInThatLang + "," + id + "," + 
					maxRetweetCount + "," + minRetweetCount + "," + retweetCount + "," + textStr + "\n")
			}
		}
	}
  
	// Pass the text of the retweet to this function to get the Language (in two letter code form) of that text.
	def getLang(s: String) : String =
	{
		val inputStr = s.replaceFirst("RT", "").replaceAll("@\\p{L}+", "").replaceAll("https?://\\S+\\s?", "")
		var langCode = new LanguageIdentifier(inputStr).getLanguage
		
		// Detect if japanese
		var pat = Pattern.compile("\\p{InHiragana}") 
		var m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ja"
		// Detect if korean
		pat = Pattern.compile("\\p{IsHangul}");
		m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ko"
		
		return langCode
	}
  
	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
  
	def main(args: Array[String]) {
    // Configure Twitter credentials

    val apiKey = "WGTqdowoERTtEj3SoIGua5hfo"
    val apiSecret = "pXRnRs0zZaSInqhxjyUs7mKftmVE3oh0hKiW20uTGvE0MQs6ZW"
    val accessToken = "	783663611512233984-HTijPuvNoEg5pd44p2LNiqvxvTFdVRK"
    val accessTokenSecret = "x0jf4i8u886i9DoXilSS5oaeXkSbV4KkCBDnZlwahBiEa"

    Helper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)
    
    
    // val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    // val ssc = new StreamingContext(sparkConf, Seconds(5))
    val ssc = new StreamingContext(new SparkConf(), Seconds(5))
    val tweets = TwitterUtils.createStream(ssc, None)

    val reTweets = tweets.filter(_.isRetweet).window(Seconds(12 * 5), Seconds(5))

    val keyValuePair = reTweets.map(status => (status.getRetweetedStatus().getId(), // will act as key
      (status.getRetweetedStatus().getRetweetCount(),
        status.getRetweetedStatus().getRetweetCount(),
        status.getRetweetedStatus().getText())))
    //keyValuePair will be like -> (IDofTweet, (TweetCountForMAX, TweetCountForMIN, TEXT) )
    
    val minMaxRDD = keyValuePair.reduceByKey((a, b) =>
                                            ( Math.max(a._1.toInt, b._1.toInt),
                                              Math.min(a._2.toInt, b._2.toInt),
                                              a._3))

    //minMaxRDD will be like -> (IDofTweet, (TweetCountForMAX, TweetCountForMIN, TEXT) )
    // it will combine all the tweets and will keep uniqe iDofTweet, max from retweetCount, min from tweetCount 

    //    minMaxRDD.cache().print() 
    // (IDofTWeet, Language)                                         
    val idLangText = reTweets.map(status => (status.getRetweetedStatus().getId(),
      getLang(status.getRetweetedStatus().getText()))) // getting language-code used by Tika library in above function
  
    val joinRDD = idLangText.join(minMaxRDD) // joining minMaxRDD and idLangText RDD ON IDofTWeet
//    joinRDD.cache().print()
    val resultV1 = joinRDD.map(rdd => (rdd._2._1,  // IDofTWeet 
     (rdd._1, // language code from idLangText
      rdd._2._2._1, // max from minMaxRdd 
      rdd._2._2._2, // min from minMaxRdd
      rdd._2._2._1 - rdd._2._2._2 + 1, // sum = max - max + 1, although it is not required for output, as it will be calculated in write2 fun, but it will be used latter to calculate sum of retweets each one langauge 
      rdd._2._2._3 // text of the tweet from _minMaxRDD 
      )))

//   result.cache().print()
       
    val groupByLanguage = resultV1.transform(rdd => rdd.groupBy(_._1))
    
    // above results transfored resultV1 RDD to 
    // // combined by language like en then a compact buffer of further values
    //          (en,CompatBuffer(en,7858543873,62,62,1, text of tweet ))

    // This will help to get Tweets Count InEach Language 

    // Now summing up the reTweetCount in each buffer and kep its as Value agains each lang code as key
    
    val groupByLanguageSum = groupByLanguage.mapValues(_.map(_._2._4.toInt).sum)
    
    //  groupByLanguageSum is like (pt,3) (th,9) (de,14) , 
    // we now we will again join this rdd to version 1 of results called resultV1 

    val resultV2 = resultV1.join(groupByLanguageSum)  // joined on Language Code 
    
    // now we have all the required aggregates but in different order
    
    val resultWithTotalRetweetCount = resultV2.map{ rdd => 
                                                  ( rdd._1, // language code
                                                    rdd._2._1._3 - rdd._2._1._4 + 1,  // Total RetweetCount, just maintaining to sort on that befhalf for once
                                                    rdd._2._2.toInt.toLong, // retweetCount in that language
                                                    rdd._2._1._1,  // ID of the Tweet
                                                    rdd._2._1._3,  // max retweets 
                                                    rdd._2._1._4,  // min retweets 
                                                    rdd._2._1._5   // text of tweet    
                                                  )}

     // 2nd pirority sort, it will sort by tottal retweet count that are computed temporaryly just to sort the data    
    val sortByRetweetCount = resultWithTotalRetweetCount.transform(rdd=> rdd.sortBy(_._2, false)) 
 
     // it will resort as per total language count, but in one language, tweets having more count will remain at top
    val sortBylang =  sortByRetweetCount.transform(rdd=> rdd.sortBy(_._3, false))  
    
    // converting output to desired into formate of writer function
    val outputRDD =  sortBylang.map{ rdd=> ( rdd._1,   // language code
                                             rdd._3,   // Total ReTweet Count in the language 
                                             rdd._4,   // ID of the Tweet
                                             rdd._7,    // text in the tweet
                                             rdd._5,   // max reTweet Count
                                             rdd._6    // min retweet Count  
                                             
                                          ) }    

    outputRDD.foreachRDD{
        rdd => write2Log(rdd.collect())
        }
    println("This is Application Twitterstats Running with all personal Tweeter credentials -------------------------")

    /*new java.io.File("cpdir").mkdirs
		ssc.checkpoint("cpdir")*/
    ssc.start()
    ssc.awaitTermination()
  }

}

