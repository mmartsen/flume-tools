package mmartsen.flume.sources.twitter;

import org.apache.flume.instrumentation.SourceCounterMBean;

public interface TwitterSourceCounterMBean extends SourceCounterMBean {
	
	long getEventRejectedCount();

	long getStallWarningsCount();

	long getTwitterExceptionsCount();
	
	long getLimitedStatusesCount();

}
