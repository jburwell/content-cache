package net.cockamamy.contentcache;

import static java.lang.String.*;
import static java.util.concurrent.TimeUnit.*;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * 
 * A content cache class designed to minimize expensive trips to a datasource
 * and to expand and contract with available memory. It also includes the
 * capability to remove items from the cache beyond a given TTL.
 * 
 * Based Brian Goetz's expandable result set cache from Java Concurrency in
 * Practice (p. 108), and enhanced to use soft references and a scheduled
 * reaper.
 * 
 * @author jburwell
 * 
 */
public final class CMSModel {

	private final DataSource myDataSource;

	private final ConcurrentMap<String, SoftReference<Future<CacheItem>>> myCache;

	private final ScheduledThreadPoolExecutor myReaperExecutor;

	public CMSModel(DataSource aDataSource, long aTTL, TimeUnit aTTLUnit) {

		super();

		assert aDataSource != null;
		assert aTTLUnit != null;

		this.myDataSource = aDataSource;
		this.myCache = new ConcurrentHashMap<String, SoftReference<Future<CacheItem>>>();

		this.myReaperExecutor = new ScheduledThreadPoolExecutor(1);
		this.myReaperExecutor.scheduleAtFixedRate(new CacheReaper(aTTL,
				aTTLUnit), 2, 1, SECONDS);
	}

	public CMSModel(DataSource aDataSource, long aTTL) {

		this(aDataSource, aTTL, SECONDS);

	}

	public Object get(String aQuery) {

		while (true) {

			SoftReference<Future<CacheItem>> aReference = this.myCache
					.get(aQuery);
			Future<CacheItem> aValueHolder = (aReference != null) ? aReference
					.get() : null;

			while (aValueHolder == null) {

				FutureTask<CacheItem> aTask = new FutureTask<CacheItem>(
						new FindValue(this.myDataSource, aQuery));

				SoftReference<Future<CacheItem>> aNewReference = new SoftReference<Future<CacheItem>>(
						aTask);

				// Protect against other threads adding a reference or replacing
				// a stale reference underneath us. If the reference is changed
				// in the the map, then the value must be re-computed (i.e.
				// retrieved from the data source) ...
				boolean aComputeFlag = false;
				synchronized (this.myCache) {

					if (aReference == null
							&& this.myCache.containsKey(aQuery) == false) {

						this.myCache.putIfAbsent(aQuery, aNewReference);
						aComputeFlag = true;

					} else {

						aComputeFlag = this.myCache.replace(aQuery, aReference,
								aNewReference);

					}

				}

				if (aComputeFlag == true) {

					aTask.run();
					aValueHolder = aTask;

				} else {

					aNewReference = this.myCache.get(aQuery);
					aValueHolder = (aNewReference != null) ? aNewReference
							.get() : null;

				}

			}

			try {

				return aValueHolder.get().getValue();

			} catch (InterruptedException e) {

				this.myCache.remove(aQuery, aReference);

			} catch (ExecutionException e) {

				throw new IllegalStateException(format(
						"Failed to retrieve value for query %1$s", aQuery), e);

			}

		}

	}

	/**
	 * 
	 * Finds the value for a query from a data source.
	 * 
	 * @author jburwell
	 * 
	 */
	private static final class FindValue implements Callable<CacheItem> {

		private final String myQuery;
		private final DataSource myDataSource;

		public FindValue(DataSource aDataSource, String aQuery) {

			super();

			assert aDataSource != null;

			this.myDataSource = aDataSource;
			this.myQuery = aQuery;

		}

		public CacheItem call() throws Exception {

			return new CacheItem(this.myDataSource.find(this.myQuery));

		}

	}

	/**
	 * 
	 * An item in the cache including its value and the time it was created.
	 * 
	 * @author jburwell
	 * 
	 */
	private final static class CacheItem {

		private final Object myValue;
		private final long myCreatedTime;

		public CacheItem(Object aValue) {

			super();

			assert aValue != null;

			this.myValue = aValue;
			this.myCreatedTime = System.nanoTime();

		}

		@Override
		public boolean equals(Object thatObject) {

			if (thatObject != null
					&& this.getClass().equals(thatObject.getClass()) == true) {

				CacheItem thatCacheItem = (CacheItem) thatObject;

				if (this.myValue.equals(thatCacheItem.getValue()) == true
						&& this.myCreatedTime == thatCacheItem.getCreatedTime()) {

					return true;

				}

			}

			return false;

		}

		@Override
		public int hashCode() {

			int aHashCode = 37;

			aHashCode += 17 * this.myValue.hashCode();
			aHashCode += 17 * (int) (this.myCreatedTime ^ (this.myCreatedTime >>> 32));

			return aHashCode;

		}

		@Override
		public String toString() {

			return format("CacheItem (createdTime: %1$s, value %2$s",
					this.myCreatedTime, this.myValue);

		}

		public final Object getValue() {

			return this.myValue;

		}

		public final long getCreatedTime() {

			return this.myCreatedTime;

		}

	}

	/**
	 * 
	 * Remove items from the cache that have been present beyond a configurable
	 * TTL.
	 * 
	 * @author jburwell
	 * 
	 */
	private final class CacheReaper implements Runnable {

		private final long myTTL;

		CacheReaper(long aTTL, TimeUnit aTTLUnit) {

			super();

			this.myTTL = aTTLUnit.convert(aTTL, NANOSECONDS);

		}

		public void run() {

			try {

				// Baseline now for use in comparisons ...
				final long now = System.nanoTime();

				// Grab the entries for traversal ...
				Set<Map.Entry<String, SoftReference<Future<CacheItem>>>> theCacheItems = myCache
						.entrySet();

				// According to the ConcurrentMap javadoc, the iterator returned
				// for the entry set is a "weakly consistent" view of the map
				// which should protect against a variety of consistent
				// modification scenarios ...
				for (Map.Entry<String, SoftReference<Future<CacheItem>>> aCacheItemEntry : theCacheItems) {

					CacheItem aCacheItem = null;
					if (aCacheItemEntry != null
							&& aCacheItemEntry.getValue() != null) {

						// In all likelihood a sub-optimal approach due to the
						// potential to block waiting for a pending calculation
						// to complete. Would perform deeper testing to
						// understand the impacts and alternative approaches ...
						aCacheItem = aCacheItemEntry.getValue().get().get();

					} else {

						// A stale reference -- remove it and move onto the next
						// entry ...
						myCache.remove(aCacheItemEntry.getKey());
						continue;
					}

					assert aCacheItem != null;

					final long anItemAge = now - aCacheItem.getCreatedTime();

					if (anItemAge > this.myTTL) {

						myCache.remove(aCacheItemEntry.getKey());

					}

				}

			} catch (ExecutionException e) {

				throw new IllegalStateException(e);

			} catch (InterruptedException e) {

				throw new IllegalStateException(e);

			}

		}
	}

}
