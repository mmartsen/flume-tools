package mmartsen.flume.sources.twitter;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * A Flume Source, which pulls data from Twitter's streaming API. Currently,
 * this only supports pulling from the sample API, and only gets new status
 * updates.
 */
public class TwitterSource extends AbstractSource implements EventDrivenSource, Configurable {

	private static final Logger logger = LoggerFactory.getLogger(TwitterSource.class);

	/** Information necessary for accessing the Twitter API */
	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String accessTokenSecret;
	private TwitterSourceCounter sourceCounter;

	private String[] keywords;
	private String[] langs;
	private long[] follow;
	private double[][] locations;

	/** The actual Twitter stream. It's set up to collect raw JSON data */
	private TwitterStream twitterStream;

	/**
	 * The initialization method for the Source. The context contains all the
	 * Flume configuration info, and can be used to retrieve any configuration
	 * values necessary to set up the Source.
	 */
	@Override
	public void configure(Context context) {
		consumerKey = context.getString(TwitterSourceConstants.CONSUMER_KEY_KEY);
		consumerSecret = context.getString(TwitterSourceConstants.CONSUMER_SECRET_KEY);
		accessToken = context.getString(TwitterSourceConstants.ACCESS_TOKEN_KEY);
		accessTokenSecret = context.getString(TwitterSourceConstants.ACCESS_TOKEN_SECRET_KEY);

		keywords = csvToArray(context.getString(TwitterSourceConstants.KEYWORDS_KEY, ""));
		langs = csvToArray(context.getString(TwitterSourceConstants.LANGS_KEY, ""));
		follow = csvToArrayLong(context.getString(TwitterSourceConstants.FOLLOWS_KEY, ""));
		processLocations(context.getString(TwitterSourceConstants.LOCATIONS_KEY, ""));

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setOAuthConsumerKey(consumerKey);
		cb.setOAuthConsumerSecret(consumerSecret);
		cb.setOAuthAccessToken(accessToken);
		cb.setOAuthAccessTokenSecret(accessTokenSecret);
		cb.setJSONStoreEnabled(true); // important to convert status to raw json format
		cb.setIncludeEntitiesEnabled(true);

		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

		if (sourceCounter == null) {
			sourceCounter = new TwitterSourceCounter(getName());
		}
	}

	/**
	 * Start processing events. This uses the Twitter Streaming API to sample
	 * Twitter, and process tweets.
	 */
	@Override
	public void start() {
		// The channel is the piece of Flume that sits between the Source and
		// Sink,
		// and is used to process events.
		final ChannelProcessor channel = getChannelProcessor();

		final Map<String, String> headers = new HashMap<String, String>();

		// The StatusListener is a twitter4j API, which can be added to a
		// Twitter stream, and will execute methods every time a message comes
		// in
		// through the stream.
		StatusListener listener = new StatusListener() {
			// The onStatus method is executed every time a new tweet comes in.
			public void onStatus(Status status) {
				// The EventBuilder is used to build an event using the headers
				// and the raw JSON of a tweet
				logger.debug(status.getUser().getScreenName() + ": " + status.getText());

				headers.put("timestamp", String.valueOf(status.getCreatedAt().getTime()));
				Event event = EventBuilder.withBody(TwitterObjectFactory.getRawJSON(status).getBytes(), headers);
				sourceCounter.incrementEventReceivedCount();
				try {
					channel.processEvent(event);
					sourceCounter.incrementEventAcceptedCount();
				} catch (ChannelException e) {
					sourceCounter.incrementEventsRejectedCount();
					throw (e);
				}
			}

			// This listener will ignore everything except for new tweets
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				logger.warn("Twitter stream get out of 1% of firehose. Number of tweets removed so far: "
						+ numberOfLimitedStatuses);
				sourceCounter.setLimitedStatusesCount(numberOfLimitedStatuses);
			}

			public void onScrubGeo(long userId, long upToStatusId) {}

			public void onException(Exception ex) {
				logger.warn("Twitter exception occured: " + ex.getMessage());
				sourceCounter.incrementTwitterExceptionsCount();
			}

			public void onStallWarning(StallWarning warning) {
				logger.warn("Twitter server side queue is almost full: " + warning.getMessage() + " %: "
						+ warning.getPercentFull());
				sourceCounter.incrementStallWarningsCount();
			}
		};

		logger.debug("Setting up Twitter sample stream using consumer key {} and" + " access token {}",
				new String[] { consumerKey, accessToken });
		// Set up the stream's listener (defined above),
		twitterStream.addListener(listener);

		// Set up a filter to pull out industry-relevant tweets
		if (keywords.length == 0 && langs.length == 0 && locations.length == 0 && follow.length == 0) {
			logger.info("Starting up Twitter sampling...");
			twitterStream.sample();
		} else {
			logger.info("Starting up Twitter filtering...");
			FilterQuery query = new FilterQuery();
			if (keywords.length>0) query.track(keywords);
			if (langs.length>0) query.language(langs);
			if (follow.length>0) query.follow(follow);
			if (locations.length>0) query.locations(locations);
			twitterStream.filter(query);
		}
		sourceCounter.start();
		super.start();
	}

	/**
	 * Stops the Source's event processing and shuts down the Twitter stream.
	 */
	@Override
	public void stop() {
		logger.debug("Shutting down Twitter sample stream...");
		twitterStream.shutdown();
		sourceCounter.stop();
		super.stop();
		logger.info("Twitter streaming source {} stopped. Metrics: {}", getName(), sourceCounter);
	}

	private void processLocations(String locations) {
		if (locations.trim().length() == 0) {
			this.locations = new double[0][0];
			return;
		}
		double[][] list;
		String[] locationsArr = locations.split(",");
		list = new double[locationsArr.length / 2][2];
		for (int i = 0; i < locationsArr.length; i = i + 2) {
			double[] pair = new double[2];
			pair[0] = Double.parseDouble(locationsArr[i]);
			pair[1] = Double.parseDouble(locationsArr[i + 1]);
			list[i / 2] = pair;
		}
		this.locations = list;
	}
	
	private String[] csvToArray(String in) {
		String [] out = new String[0];
		if (in.trim().length() > 0) {
			out = in.split(",");
			for (int i = 0; i < out.length; i++) {
				out[i] = out[i].trim();
			}
		}
		return out;
	}
	
	private long[] csvToArrayLong(String in) {
		long[] out = new long[0];
		if (in.trim().length() > 0) {
			String [] outStr = in.split(",");
			for (int i = 0; i < outStr.length; i++) {
				out[i] = Long.parseLong(outStr[i].trim());
			}
		}
		return out;
	}
	
	private class TwitterSourceConstants {
		public static final String CONSUMER_KEY_KEY = "consumerKey";
		public static final String CONSUMER_SECRET_KEY = "consumerSecret";
		public static final String ACCESS_TOKEN_KEY = "accessToken";
		public static final String ACCESS_TOKEN_SECRET_KEY = "accessTokenSecret";

		public static final String KEYWORDS_KEY = "keywords";
		public static final String LANGS_KEY = "language";
		public static final String LOCATIONS_KEY = "locations";
		public static final String FOLLOWS_KEY = "follow";
	}
}
