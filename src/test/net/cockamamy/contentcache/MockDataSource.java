package net.cockamamy.contentcache;

import static java.lang.String.*;
import static java.util.Collections.*;

import java.util.*;
import java.util.concurrent.atomic.*;

final class MockDataSource implements DataSource {

	private final Map<String, Object> myValues;

	private final AtomicInteger myHitCount;
	
	public MockDataSource(Map<String, Object> theValues) {

		this.myValues = unmodifiableMap(theValues);
		this.myHitCount = new AtomicInteger(0);
		
	}

	public Object find(String aQuery) {

		// Simulate an expensive operation ...
		while (true) {

			this.myHitCount.incrementAndGet();
			
			try {

				Thread.sleep(1000);

			} catch (InterruptedException e) {

				throw new IllegalStateException(format(
						"Get operation for query %1$s interrupted", aQuery), e);

			}

			return this.myValues.get(aQuery);

		}

	}

	public int getHitCount() {
		
		return this.myHitCount.get();
		
	}

}
