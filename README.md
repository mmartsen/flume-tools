# flume-tools
Set of custom flume components to support various usage scenarios

# Twitter Source usage
## Flume conf file example
```INI
TwitterAgent.sources = PublicStream
TwitterAgent.channels = MemCh
TwitterAgent.sinks = HDFS

TwitterAgent.sources.PublicStream.type = mmartsen.flume.sources.twitter.TwitterSource
TwitterAgent.sources.PublicStream.channels = MemCh
TwitterAgent.sources.PublicStream.consumerKey = %consumer_key%
TwitterAgent.sources.PublicStream.consumerSecret = %consumer_secret%
TwitterAgent.sources.PublicStream.accessToken = %access_token%
TwitterAgent.sources.PublicStream.accessTokenSecret = %token_secret%
TwitterAgent.sources.PublicStream.keywords = obama,@realDonaldTrump,#somehashtag
# USA boundaries
TwitterAgent.sources.PublicStream.locations = -179.231086,13.182335,179.859685,71.434357
TwitterAgent.sources.PublicStream.language = en,de,fr
TwitterAgent.sources.PublicStream.follow = 813286,1536791610,737904218

TwitterAgent.sinks.HDFS.channel = MemCh
TwitterAgent.sinks.HDFS.type = hdfs
TwitterAgent.sinks.HDFS.hdfs.path = /user/flume/Twitter/PublicStream/day_key=%Y%m%d/
TwitterAgent.sinks.HDFS.hdfs.fileType = CompressedStream
TwitterAgent.sinks.HDFS.hdfs.codeC = gzip
TwitterAgent.sinks.HDFS.hdfs.filePrefix = PublicStream
TwitterAgent.sinks.HDFS.hdfs.writeFormat = Text
TwitterAgent.sinks.HDFS.hdfs.maxOpenFiles = 10
TwitterAgent.sinks.HDFS.hdfs.batchSize = 10
TwitterAgent.sinks.HDFS.hdfs.rollSize = 0
TwitterAgent.sinks.HDFS.hdfs.rollCount = 5000
TwitterAgent.sinks.HDFS.hdfs.rollInterval = 600

TwitterAgent.channels.MemCh.type = memory
TwitterAgent.channels.MemCh.capacity = 10000
TwitterAgent.channels.MemCh.transactionCapacity = 100
```