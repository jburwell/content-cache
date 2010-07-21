package net.cockamamy.contentcache;

import static org.testng.Assert.*;

import java.util.*;

import org.testng.annotations.*;

@Test
public final class CMSModelHerdingTest {

	private final static String GET_DATA_PROVIDER = "cmsmodel_get_data";

	private static final Object REF_VALUE = Integer.valueOf(42);

	private static final String REF_QUERY = "What is the meaning of life?";

	private MockDataSource myDataSource;

	private CMSModel myModel;

	@BeforeClass
	public void setup() {

		Map<String, Object> theValues = new HashMap<String, Object>();
		theValues.put(REF_QUERY, REF_VALUE);

		this.myDataSource = new MockDataSource(theValues);
		this.myModel = new CMSModel(this.myDataSource, 10);

	}

	@Test(groups="herd", dataProvider = GET_DATA_PROVIDER, threadPoolSize = 10)
	public void testGet(String aQuery, Object anExpectedValue) {

		assertSame(this.myModel.get(aQuery), anExpectedValue);

	}

	@Test(dependsOnGroups = "herd")
	public void verifyDataSourceHits() {

		assertEquals(this.myDataSource.getHitCount(), 1);

	}

	@DataProvider(name = GET_DATA_PROVIDER, parallel = true)
	public Object[][] provideGetData() {

		final int aNumRequests = 1000;
		Object[][] theData = new Object[aNumRequests][2];

		for (int i = 0; i < aNumRequests; i++) {

			theData[i][0] = REF_QUERY;
			theData[i][1] = REF_VALUE;

		}

		return theData;

	}
}
