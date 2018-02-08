# Twitter-Data-Analysis-Using-Kibana
Twitter data analysis using kibana and elasticsearch

Creating the Twitter Source
Create the FlinkTwitterStreamCount.scala file with following content:
import java.util.Properties
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.twitter.TwitterSource

object FlinkTwitterStreamCount {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.setProperty(TwitterSource.CONSUMER_KEY, "<your-value>")
    props.setProperty(TwitterSource.CONSUMER_SECRET, "<your-value>")
    props.setProperty(TwitterSource.TOKEN, "<your-value>")
    props.setProperty(TwitterSource.TOKEN_SECRET, <your-value>")
    val streamSource = env.addSource(new TwitterSource(props))
    
    streamSource.print()
    
    env.execute("Twitter Window Stream WordCount")
  }
}

We setup a TwitterSource reader proving all the authentication details. 
Compile and execute our program by clicking onto the Run button. This emits a string with random Twitter records in JSON format, 
e.g.:
{
  "delete": {
    "status": {
      "id": 715433581762359296,
      "id_str": "715433581762359296",
      "user_id": 362889585,
      "user_id_str": "362889585"
    },
    "timestamp_ms": "1474193375156"
  }
}

#Externalising the Twitter Connection Details
twitter-source.consumerKey=<your-details>
twitter-source.consumerSecret=<your-details>
twitter-source.token=<your-details>
twitter-source.tokenSecret=<your-details>

We will add following code to our Scala file:
val prop = new Properties()
val propFilePath = "/home/algobasket/Dropbox/development/config/ter.properties"

  try {

    prop.load(new FileInputStream(propFilePath))
    prop.getProperty("twitter-source.consumerKey")
    prop.getProperty("twitter-source.consumerSecret")
    prop.getProperty("twitter-source.token")
    prop.getProperty("twitter-source.tokenSecret")

  } catch { case e: Exception =>
    e.printStackTrace()
    sys.exit(1)
  }

val streamSource = env.addSource(new TwitterSource(prop))
Ok, that’s this done then. We can no progress with a good conscience.
#Handling the returned JSON stream
val filteredStream = streamSource.filter( value =>  value.contains("created_at"))

val parsedStream = filteredStream.map(
  record => {
    parse(record)
  }
)

//parsedStream.print()

case class TwitterFeed(
  id:Long
  , creationTime:Long
  , language:String
  , user:String
  , favoriteCount:Int
  , retweetCount:Int
)

val structuredStream:DataStream[TwitterFeed] = parsedStream.map(
  record => {
    TwitterFeed(
      // ( input \ path to element \\ unboxing ) (extract no x element from list)
      ( record \ "id" \\ classOf[JInt] )(0).toLong
      , DateTimeFormat
          .forPattern("EEE MMM dd HH:mm:ss Z yyyy")
          .parseDateTime(
            ( record \ "created_at" \\ classOf[JString] )(0)
          ).getMillis
      , ( record \ "lang" \\ classOf[JString] )(0).toString
      , ( record \ "user" \ "name" \\ classOf[JString] )(0).toString
      , ( record \ "favorite_count" \\ classOf[JInt] )(0).toInt
      , ( record \ "retweet_count" \\ classOf[JInt] )(0).toInt
    )

  }
)

structuredStream.print

So what is exactly happening here?

First we discard Twitter delete records by using the filter function.
Next we create a TwitterFeed class which will represent the typed fields that we want to keep for our analysis.
Then we apply a map function to extract the required fields and map them to the TwitterFeed class. We use the Lift functions to get hold of the various fields: First we define the input (record in our case), then the path to the JSON element followed by the unboxing function (which turns the result from a Lift type to a standard Scala type). The result is returned as a List (because if you in example lazily define the element path, there could be more than one of these elements in the JSON object). In our case, we define the absolute path the JSON element, so we are quite certain that only one element will be returned, hence we can extract the first element of the list (achieved by calling (0)). One other important point here is that we apply a Date Pattern to the created_at String and then convert it to Milliseconds (since epoch) using getMillis: Dates/Time has to be converted to Milliseconds because Apache Flink requires them in this format. We will discuss the date conversion shortly in a bit more detail.
The output of the stream looks like this:
1> TwitterFeed(787307728771440640,1476543764000,en,Sean Guidera,0,0)
3> TwitterFeed(787307728767025152,1476543764000,en,spooky!kasya,0,0)

#Converting String to Date
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.14.0"
And you will have to add to your Scala file the following import statement:
We apply a formatting mask and if we need some help on this, we can take a look at theDocumentation.
The create_at value looks something like this: Sun Sep 18 10:09:34 +0000 2016, so the corresponding formatting mask is 
EE MMM dd HH:mm:ss Z yyyy:
DateTimeFormat
.forPattern("EEE MMM dd HH:mm:ss Z yyyy")
.parseDateTime(
  ( record \ "created_at" \\ classOf[JString] )(0)
).getMillis

#Creating a Tumbling Window Aggregation
val recordSlim:DataStream[Tuple2[String, Int]] = structuredStream.map(
  value => (
    value.language
    , 1
    )
)

// recordSlim.print

val counts = recordSlim
  .keyBy(0)
  .timeWindow(Time.seconds(30))
  .sum(1)

counts.print
The output looks like this:

1> (et,1)
2> (tl,19)
4> (pt,72)
3> (en,433)
4> (fr,23)
2> (el,2)
1> (vi,1)
4> (in,29)
3> (no,1)
1> (ar,101)

#Full Sink Implementation
# delete index if already exists
  curl -XDELETE 'http://localhost:9200/tweets'
  curl -XDELETE 'http://localhost:9200/tweetsbylanguage'
# create index
  curl -XPUT 'http://localhost:9200/tweets'
  curl -XPUT 'http://localhost:9200/tweetsbylanguage'
# create mapping for lowest granularity data
curl -XPUT 'http://localhost:9200/tweets/_mapping/partition1' -d'
{
  "partition1" : {
    "properties" : {
    "id": {"type": "long"}
    , "creationTime": {"type": "date"}
    , "language": {"type": "string", "index": "not_analyzed"}
    , "user": {"type": "string"}
    , "favoriteCount": {"type": "integer"}
    , "retweetCount": {"type": "integer"}
    , "count": {"type": "integer"}
  }
  }
}'

curl -XPUT 'http://localhost:9200/tweets/_settings' -d '{
    "index" : {
        "refresh_interval" : "5s"
    }
}'
# create mapping for tweetsByLanguage
curl -XPUT 'http://localhost:9200/tweetsbylanguage/_mapping/partition1' -d'
{
  "partition1" : {
    "properties" : {
    "language": {"type": "string", "index": "not_analyzed"}
    , "windowStartTime": {"type": "date"}
    , "windowEndTime": {"type": "date"}
    , "countTweets": {"type": "integer"}
  }
  }
}'

curl -XPUT 'http://localhost:9200/tweetsbylanguage/_settings' -d '{
    "index" : {
        "refresh_interval" : "5s"
    }
}'
We will add two ElasticSearch Sinks to our Scala code: One for the low granularity data and one for the aggregate. At the same time we will also improve our windowed aggregation code a bit by assigning a case class:

val config = new util.HashMap[String, String]
config.put("bulk.flush.max.actions", "1")
config.put("cluster.name", "elasticsearch") //default cluster name: elasticsearch

val transports = new util.ArrayList[InetSocketAddress]
transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))

timedStream.addSink(new ElasticsearchSink(config, transports, new ElasticsearchSinkFunction[TwitterFeed] {
  def createIndexRequest(element:TwitterFeed): IndexRequest = {
    val mapping = new util.HashMap[String, AnyRef]
    // use LinkedHashMap if for some reason you want to maintain the insert order
    // val mapping = new util.LinkedHashMap[String, AnyRef]
    // Map stream fields to JSON properties, format:
    // json.put("json-property-name", streamField)
    // the streamField type has to be converted from a Scala to a Java Type

    mapping.put("id", new java.lang.Long(element.id))
    mapping.put("creationTime", new java.lang.Long(element.creationTime))
    mapping.put("language", element.language)
    mapping.put("user", element.user)
    mapping.put("favoriteCount", new Integer((element.favoriteCount)))
    mapping.put("retweetCount", new Integer((element.retweetCount)))
    mapping.put("count", new Integer((element.count)))

    //println("loading: " + mapping)

    Requests.indexRequest.index("tweets").`type`("partition1").source(mapping)

  }

  override def process(
      element: TwitterFeed
      , ctx: RuntimeContext
      , indexer: RequestIndexer
    )
    {
      try{
        indexer.add(createIndexRequest(element))
      } catch {
        case e:Exception => println{
          println("an exception occurred: " + ExceptionUtils.getStackTrace(e))
        }
        case _:Throwable => println("Got some other kind of exception")
      }
    }
}))

case class TweetsByLanguage (
  language:String
  , windowStartTime:Long
  , windowEndTime:Long
  , countTweets:Int
)

val tweetsByLanguageStream:DataStream[TweetsByLanguage] = timedStream
  // .keyBy("language") did not work as apparently type is not picked up
  // for the key in the apply function
  // see http://stackoverflow.com/questions/36917586/cant-apply-custom-functions-to-a-windowedstream-on-flink
  .keyBy(in => in.language)
  .timeWindow(Time.seconds(30))
  .apply
  {
    (
      // tuple with key of the window
      lang: String
      // TimeWindow object which contains details of the window
      // e.g. start and end time of the window
      , window: TimeWindow
      // Iterable over all elements of the window
      , events: Iterable[TwitterFeed]
      // collect output records of the WindowFunction
      , out: Collector[TweetsByLanguage]
    ) =>
      out.collect(
        // TweetsByLanguage( lang, window.getStart, window.getEnd, events.map( _.retweetCount ).sum )
        TweetsByLanguage( lang, window.getStart, window.getEnd, events.map( _.count ).sum )
      )
  }

// tweetsByLanguage.print

tweetsByLanguageStream.addSink(
  new ElasticsearchSink(
    config
    , transports
    , new ElasticsearchSinkFunction[TweetsByLanguage] {

      def createIndexRequest(element:TweetsByLanguage): IndexRequest = {
        val mapping = new util.HashMap[String, AnyRef]

        mapping.put("language", element.language)
        mapping.put("windowStartTime", new Long(element.windowStartTime))
        mapping.put("windowEndTime", new Long(element.windowEndTime))
        mapping.put("countTweets", new java.lang.Integer(element.countTweets))


        // println("loading: " + mapping)
        // problem: wrong order of fields, id seems to be wrong type in general, as well as retweetCount
        Requests.indexRequest.index("tweetsbylanguage").`type`("partition1").source(mapping)

      }

      override def process(
        element: TweetsByLanguage
        , ctx: RuntimeContext
        , indexer: RequestIndexer
        )
        {
          try{
            indexer.add(createIndexRequest(element))
          } catch {
            case e:Exception => println{
              println("an exception occurred: " + ExceptionUtils.getStackTrace(e))
            }
            case _:Throwable => println("Got some other kind of exception")
        }
      }
    }
  )
)

#ElasticSearch Log:
less /var/log/elasticsearch/elasticsearch.log
In my case I found a message shown below
[2018-02-02 17:45:09,306][DEBUG][action.bulk              ] [Sleek] [twitter][3] failed to execute bulk item (index) index {[twitter][languages][AVe0oir-LpV6XBRrSKbo], source[{"creationTime":"1476204303000","language":"ko","id":"7.8588392611606938E17","retweetCount":"0.0"}]}
MapperParsingException[failed to parse [language]]; nested: NumberFormatException[For input string: "ko"];

#Check if the data is stored in ES:
curl -XGET 'http://localhost:9200/tweets/_search?pretty'
The result should look something like this (after all the metadata):
}, {
  "_index" : "tweets",
  "_type" : "partition1",
  "_id" : "AVftNj2olHqgUwl-blye",
  "_score" : 1.0,
  "_source" : {
    "creationTime" : 1477153522000,
    "count" : 1,
    "language" : "en",
    "id" : 789865239164571649,
    "user" : "nnn",
    "retweetCount" : 0,
    "favoriteCount" : 0
  }
} 

#Visualising results with Kibana

