/*
 * Copyright 2025 Uppsala University Library
 *
 * This file is part of Cora.
 *
 *     Cora is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     Cora is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with Cora.  If not, see <http://www.gnu.org/licenses/>.
 */
package se.uu.ub.cora.basicstorage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.storage.Condition;
import se.uu.ub.cora.storage.Filter;
import se.uu.ub.cora.storage.Part;
import se.uu.ub.cora.storage.RelationalOperator;

public class CollectedTermsInMemoryStorageTest {

	private static final String RECORD_TYPE = "recordType";
	private static final String RECORD_ID = "recordId";
	private static final String DATA_DIVIDER = "someDataDivider";
	private CollectedTermsHolder storage;

	@BeforeMethod
	private void beforeMethod() {
		storage = new CollectedTermsInMemoryStorage();
	}

	@Test
	public void testStoreCollectedTerms_emptyStorage() {
		Set<StorageTerm> collectTermsReturned = storage.getCollectTerms(RECORD_TYPE, RECORD_ID);

		assertTrue(collectTermsReturned.isEmpty());
	}

	@Test
	public void testStoreCollectedTerms_storedWithEmptySet() {
		storage.storeCollectedTerms(RECORD_TYPE, RECORD_ID, Collections.emptySet(), DATA_DIVIDER);

		Set<StorageTerm> collectTermsReturned = storage.getCollectTerms(RECORD_TYPE, RECORD_ID);

		assertInternalMapForStorageTermsIsEmpty();
		assertTrue(collectTermsReturned.isEmpty());
	}

	private void assertInternalMapForStorageTermsIsEmpty() {
		var originalStoredStorageTerms = ((CollectedTermsInMemoryStorage) storage)
				.onlyForTestGetOriginalStoredStorageTerms();
		assertTrue(originalStoredStorageTerms.isEmpty());
	}

	private void assertInternalMapForStorageTermsIsFilled() {
		var originalStoredStorageTerms = ((CollectedTermsInMemoryStorage) storage)
				.onlyForTestGetOriginalStoredStorageTerms();
		assertFalse(originalStoredStorageTerms.isEmpty());
	}

	@Test
	public void testStoreCollectedTerms_NotFoundInStorage_differentTypeAndId() {
		Set<StorageTerm> collectedData = createCollectedDataWithUppsalaAndOckelboStorageTerm();
		storage.storeCollectedTerms(RECORD_TYPE, RECORD_ID, collectedData, DATA_DIVIDER);

		Set<StorageTerm> collectTermsReturned = storage.getCollectTerms("someOtherRecordType",
				"someOtherRecordId");

		assertInternalMapForStorageTermsIsFilled();
		assertTrue(collectTermsReturned.isEmpty());
	}

	@Test
	public void testStoreCollectedTerms_NotFoundInStorage_differentId() {
		Set<StorageTerm> collectedData = createCollectedDataWithUppsalaAndOckelboStorageTerm();
		storage.storeCollectedTerms(RECORD_TYPE, RECORD_ID, collectedData, DATA_DIVIDER);

		Set<StorageTerm> collectTermsReturned = storage.getCollectTerms(RECORD_TYPE,
				"someOtherRecordId");

		assertTrue(collectTermsReturned.isEmpty());
	}

	@Test
	public void testStoreCollectedTerms_NotFoundInStorage_differentType() {
		Set<StorageTerm> collectedData = createCollectedDataWithUppsalaAndOckelboStorageTerm();
		storage.storeCollectedTerms(RECORD_TYPE, RECORD_ID, collectedData, DATA_DIVIDER);

		Set<StorageTerm> collectTermsReturned = storage.getCollectTerms("someOtherType", RECORD_ID);

		assertTrue(collectTermsReturned.isEmpty());
	}

	@Test
	public void testStoreCollectedTerms_FoundInStorage() {
		Set<StorageTerm> collectedData = createCollectedDataWithUppsalaAndOckelboStorageTerm();
		storage.storeCollectedTerms(RECORD_TYPE, RECORD_ID, collectedData, DATA_DIVIDER);

		Set<StorageTerm> collectTermsReturned = storage.getCollectTerms(RECORD_TYPE, RECORD_ID);

		assertInternalMapForStorageTermsIsFilled();
		assertEquals(collectTermsReturned, collectedData);
	}

	@Test
	public void testStoreCollectedTerms_StoreTermsUpdated_FoundInStorage() {
		Set<StorageTerm> firstCollectedData = createCollectedDataWithUppsalaAndOckelboStorageTerm();
		storage.storeCollectedTerms(RECORD_TYPE, RECORD_ID, firstCollectedData, DATA_DIVIDER);
		Set<StorageTerm> updatedCollectedData = createCollectedDataWithStockholmAndMalmoStorageTerm();
		storage.storeCollectedTerms(RECORD_TYPE, RECORD_ID, updatedCollectedData, DATA_DIVIDER);

		Set<StorageTerm> collectTermsReturned = storage.getCollectTerms(RECORD_TYPE, RECORD_ID);

		assertEquals(collectTermsReturned, updatedCollectedData);
	}

	@Test
	public void testStoreCollectedTerms_StorageTermsUpdatedWithEmptySet() {
		Set<StorageTerm> firstCollectedData = createCollectedDataWithUppsalaAndOckelboStorageTerm();
		storage.storeCollectedTerms(RECORD_TYPE, RECORD_ID, firstCollectedData, DATA_DIVIDER);
		storage.storeCollectedTerms(RECORD_TYPE, RECORD_ID, Collections.emptySet(), DATA_DIVIDER);

		Set<StorageTerm> collectTermsReturned = storage.getCollectTerms(RECORD_TYPE, RECORD_ID);

		assertInternalMapForStorageTermsIsEmpty();
		assertTrue(collectTermsReturned.isEmpty());
	}

	@Test
	public void testDeleteStorageTerms_NotFoundInStorage() {
		Set<StorageTerm> firstCollectedData = createCollectedDataWithUppsalaAndOckelboStorageTerm();
		storage.storeCollectedTerms(RECORD_TYPE, RECORD_ID, firstCollectedData, DATA_DIVIDER);

		storage.removePreviousCollectedStorageTerms("someOtherRecordType", "someOtherRecordId");

		assertInternalMapForStorageTermsIsFilled();
	}

	@Test
	public void testDeleteStorageTerms_StorageTermsDeleted() {
		Set<StorageTerm> firstCollectedData = createCollectedDataWithUppsalaAndOckelboStorageTerm();
		storage.storeCollectedTerms(RECORD_TYPE, RECORD_ID, firstCollectedData, DATA_DIVIDER);

		storage.removePreviousCollectedStorageTerms(RECORD_TYPE, RECORD_ID);
		Set<StorageTerm> collectTerms = storage.getCollectTerms(RECORD_TYPE, RECORD_ID);

		assertInternalMapForStorageTermsIsEmpty();
		assertTrue(collectTerms.isEmpty());
	}

	@Test
	public void testFindRecordIdsForFilter_concurrentStoreAndRead_noConcurrentModificationException()
			throws InterruptedException, ExecutionException {
		CollectedTermsInMemoryStorage concurrentStorage = new CollectedTermsInMemoryStorage();
		Set<StorageTerm> collectedData = createCollectedDataWithManyStorageTermsForSameKey(5000);
		Filter filter = createFilterWithAPart("placeName", "value2500");

		ExecutorService executorService = Executors.newFixedThreadPool(2);
		CountDownLatch startLatch = new CountDownLatch(1);
		try {
			Future<?> storeFuture = executorService.submit(() -> {
				waitForStart(startLatch);
				for (int index = 0; index < 25; index++) {
					concurrentStorage.storeCollectedTerms(RECORD_TYPE, RECORD_ID, collectedData,
							DATA_DIVIDER);
				}
			});
			Future<?> readFuture = executorService.submit(() -> {
				waitForStart(startLatch);
				for (int index = 0; index < 500; index++) {
					List<String> foundRecordIds = concurrentStorage.findRecordIdsForFilter(RECORD_TYPE,
							filter);
					assertTrue(foundRecordIds.isEmpty() || foundRecordIds.contains(RECORD_ID));
				}
			});

			startLatch.countDown();

			storeFuture.get();
			readFuture.get();
		} finally {
			executorService.shutdownNow();
			assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
		}
	}

	private Set<StorageTerm> createCollectedDataWithUppsalaAndOckelboStorageTerm() {
		StorageTerm storageTerm1 = new StorageTerm("placeNameStorageTerm", "placeName", "Uppsala");
		StorageTerm storageTerm2 = new StorageTerm("placeNameStorageTerm", "placeName2", "Ockelbo");
		return Set.of(storageTerm1, storageTerm2);
	}

	private Set<StorageTerm> createCollectedDataWithStockholmAndMalmoStorageTerm() {
		StorageTerm storageTerm1 = new StorageTerm("placeNameStorageTerm", "placeName",
				"Stockholm");
		StorageTerm storageTerm2 = new StorageTerm("placeNameStorageTerm", "placeName2", "Malmö");
		return Set.of(storageTerm1, storageTerm2);
	}

	private Set<StorageTerm> createCollectedDataWithManyStorageTermsForSameKey(int numberOfTerms) {
		Set<StorageTerm> storageTerms = new LinkedHashSet<>();
		for (int index = 0; index < numberOfTerms; index++) {
			storageTerms.add(new StorageTerm("placeNameStorageTerm" + index, "placeName",
					"value" + index));
		}
		return storageTerms;
	}

	private Filter createFilterWithAPart(String key, String value) {
		Condition condition = new Condition(key, RelationalOperator.EQUAL_TO, value);

		Part part = new Part();
		part.conditions.add(condition);

		Filter filter = new Filter();
		filter.include.add(part);

		return filter;
	}

	private void waitForStart(CountDownLatch startLatch) {
		try {
			startLatch.await();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		}
	}

}
