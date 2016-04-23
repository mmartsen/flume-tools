package mmartsen.flume.sources.twitter;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flume.instrumentation.SourceCounter;

public class TwitterSourceCounter extends SourceCounter {

	private static final String COUNTER_EVENTS_REJECTED = "src.events.rejected";
	private static final String COUNTER_STALL_WARNINGS = "src.stallwarnings.count";
	private static final String COUNTER_TWITTER_EXCEPTIONS = "src.twitter.exceptions.count";
	private static final String COUNTER_LIMITED_STATUSES = "src.limited.statuses.count";

	private static final String[] ATTRIBUTES = { COUNTER_EVENTS_REJECTED, COUNTER_STALL_WARNINGS,
			COUNTER_TWITTER_EXCEPTIONS, COUNTER_LIMITED_STATUSES };

	public TwitterSourceCounter(String name) {
		super(name, ATTRIBUTES);
	}

	public TwitterSourceCounter(String name, String[] attributes) {
		super(name, (String[]) ArrayUtils.addAll(attributes, ATTRIBUTES));
	}

	public void setLimitedStatusesCount(long limitedStatusesCount) {
		set(COUNTER_LIMITED_STATUSES, limitedStatusesCount);
	}

	public long incrementStallWarningsCount() {
		return increment(COUNTER_STALL_WARNINGS);
	}
	
	public long incrementTwitterExceptionsCount() {
		return increment(COUNTER_TWITTER_EXCEPTIONS);
	}
	
	public long incrementEventsRejectedCount() {
		return increment(COUNTER_EVENTS_REJECTED);
	}
}
