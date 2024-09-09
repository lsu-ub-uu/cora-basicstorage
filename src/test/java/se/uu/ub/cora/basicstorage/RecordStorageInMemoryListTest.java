/*
 * Copyright 2015, 2017, 2018, 2023, 2024 Uppsala University Library
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
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.basicstorage.testdata.DataCreator;
import se.uu.ub.cora.basicstorage.testdata.TestDataRecordInMemoryStorage;
import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.DataProvider;
import se.uu.ub.cora.data.DataRecordGroup;
import se.uu.ub.cora.data.collected.Link;
import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.data.copier.DataCopierProvider;
import se.uu.ub.cora.data.spies.DataFactorySpy;
import se.uu.ub.cora.data.spies.DataGroupSpy;
import se.uu.ub.cora.storage.Condition;
import se.uu.ub.cora.storage.Filter;
import se.uu.ub.cora.storage.Part;
import se.uu.ub.cora.storage.RelationalOperator;
import se.uu.ub.cora.storage.StorageReadResult;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;
import se.uu.ub.cora.testutils.mrv.MethodReturnValues;

public class RecordStorageInMemoryListTest {
	private RecordStorageInMemory recordStorage;
	private Set<Link> emptyLinkList = Collections.emptySet();
	Filter emptyFilter = new Filter();
	private Set<StorageTerm> storageTerms = Collections.emptySet();
	private String dataDivider = "cora";
	private DataCopierFactorySpy dataCopierFactory;
	private DataFactorySpy dataFactorySpy;

	@BeforeMethod
	public void beforeMethod() {
		dataFactorySpy = new DataFactorySpy();
		DataProvider.onlyForTestSetDataFactory(dataFactorySpy);

		dataCopierFactory = new DataCopierFactorySpy();
		DataCopierProvider.setDataCopierFactory(dataCopierFactory);

		recordStorage = new RecordStorageInMemory();
		DataGroup recordTypeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("place", "true", "false");
		recordStorage.create("recordType", "place", recordTypeRecordType, storageTerms,
				emptyLinkList, "cora");
	}

	@Test
	public void testListWithFilterButNoDataForTheType() {
		Filter filter = createFilterWithAPart("placeName", "Uppsala");

		StorageReadResult readResult = recordStorage.readList(List.of("place"), filter);

		assertEquals(readResult.listOfDataGroups.size(), 0);
	}

	private Filter createFilterWithAPart(String key, String value) {
		Condition condition = new Condition(key, RelationalOperator.EQUAL_TO, value);

		Part part = new Part();
		part.conditions.add(condition);

		Filter filter = new Filter();
		filter.include.add(part);

		return filter;
	}

	@Test
	public void testListWithCollectedStorageTermReadWithEmptyFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();

		StorageReadResult readResult = recordStorage.readList(List.of("place"), emptyFilter);
		Collection<DataGroup> readList = readResult.listOfDataGroups;

		assertEquals(readList.size(), 2);
		assertEquals(readResult.start, 0);
		assertEquals(readResult.totalNumberOfMatches, 2);
	}

	@Test
	public void testIndependentDataListWithCollectedStorageTermReadWithEmptyFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("createNewWhenCopyingThisTopLevelGroup");
		createPlaceInStorageWithStockholmStorageTerm();

		Collection<DataGroup> readList = recordStorage.readList(List.of("place"),
				emptyFilter).listOfDataGroups;
		Collection<DataGroup> readList2 = recordStorage.readList(List.of("place"),
				emptyFilter).listOfDataGroups;
		DataGroup first = readList.iterator().next();
		DataGroup secondRead = readList2.iterator().next();
		assertNotSame(first, secondRead);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithNonMatchingFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();

		Filter filter = createFilterWithAPart("placeName", "NOT_UPPSALA");

		StorageReadResult readResult = recordStorage.readList(List.of("place"), filter);
		Collection<DataGroup> readList = readResult.listOfDataGroups;
		assertEquals(readList.size(), 0);
		assertEquals(readResult.start, 0);
		assertEquals(readResult.totalNumberOfMatches, 0);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithNonExisitingKeyFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();

		Filter filter = createFilterWithAPart("NOT_placeName", "Uppsala");

		Collection<DataGroup> readList = recordStorage.readList(List.of("place"),
				filter).listOfDataGroups;
		assertEquals(readList.size(), 0);
	}

	@Test
	public void testListWithNoCollectedStorageTermReadWithFilter() {
		createPlaceInStorageWithCollectedData(storageTerms, "nameInData");

		Filter filter = createFilterWithAPart("placeName", "Uppsala");

		Collection<DataGroup> readList = recordStorage.readList(List.of("place"),
				filter).listOfDataGroups;
		assertEquals(readList.size(), 0);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithMatchingUppsalaFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();

		Filter filter = createFilterWithAPart("placeName", "Uppsala");

		StorageReadResult readList = recordStorage.readList(List.of("place"), filter);

		assertEquals(readList.start, 0);
		assertEquals(readList.totalNumberOfMatches, 1);
		assertEquals(readList.listOfDataGroups.size(), 1);
		DataGroup first = readList.listOfDataGroups.iterator().next();

		dataCopierFactory.MCR.assertNumberOfCallsToMethod("factorForDataElement", 4);

		DataCopierSpy dcs3 = (DataCopierSpy) dataCopierFactory.MCR
				.getReturnValue("factorForDataElement", 3);

		dcs3.MCR.assertNumberOfCallsToMethod("copy", 1);

		dcs3.MCR.assertReturn("copy", 0, first);
	}

	@Test
	public void testIndependentDataListWithCollectedStorageTermReadWithMatchingUppsalaFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("createNewWhenCopyingThisTopLevelGroup");
		createPlaceInStorageWithStockholmStorageTerm();

		Filter filter = createFilterWithAPart("placeName", "Uppsala");

		Collection<DataGroup> readList = recordStorage.readList(List.of("place"),
				filter).listOfDataGroups;
		Collection<DataGroup> readList2 = recordStorage.readList(List.of("place"),
				filter).listOfDataGroups;
		DataGroup first = readList.iterator().next();
		DataGroup secondRead = readList2.iterator().next();
		assertNotSame(first, secondRead);
	}

	@Test
	public void testCollectedStorageTermReadWithMatchingUppsalaOckelboFilter() {
		createPlaceInStorageWithUppsalaAndOckelboStorageTerm("someNameInData");
		createPlaceInStorageWithUppsalaStorageTerm("whatEver");

		Filter filter = createFilterWithConditions(new ConditionValues("placeName", "Uppsala"),
				new ConditionValues("placeName2", "Ockelbo"));

		Collection<DataGroup> readList = recordStorage.readList(List.of("place"),
				filter).listOfDataGroups;

		assertEquals(readList.size(), 1);
	}

	private void createPlaceInStorageWithUppsalaAndOckelboStorageTerm(String nameInData) {
		Set<StorageTerm> storageTerms = createCollectedDataWithUppsalaAndOckelboStorageTerm();
		createPlaceInStorageWithIdAndCollectedData("place:xyz", storageTerms, nameInData);
	}

	private void createPlaceInStorageWithIdAndCollectedData(String id,
			Set<StorageTerm> storageTerms2, String nameInData) {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(nameInData,
						"place", id);
		recordStorage.create("place", id, dataGroup, storageTerms2, emptyLinkList, dataDivider);
	}

	private Set<StorageTerm> createCollectedDataWithUppsalaAndOckelboStorageTerm() {
		StorageTerm storageTerm1 = new StorageTerm("placeNameStorageTerm", "placeName", "Uppsala");
		StorageTerm storageTerm2 = new StorageTerm("placeNameStorageTerm", "placeName2", "Ockelbo");
		Set<StorageTerm> storageTerms = Set.of(storageTerm1, storageTerm2);
		return storageTerms;
	}

	private Filter createFilterWithConditions(ConditionValues... conditionValues) {
		Part part = new Part();
		for (ConditionValues partValue : conditionValues) {
			Condition condition = new Condition(partValue.key, RelationalOperator.EQUAL_TO,
					partValue.value);
			part.conditions.add(condition);
		}

		Filter filter = new Filter();
		filter.include.add(part);

		return filter;
	}

	record ConditionValues(String key, String value) {
	}

	@Test
	public void testListAfterUpdateWithNoCollectedStorageTermReadWithFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		updateUppsalaPlaceInStorageWithCollectedData(storageTerms);

		Filter filter = createFilterWithAPart("placeName", "Uppsala");

		Collection<DataGroup> readList = recordStorage.readList(List.of("place"),
				filter).listOfDataGroups;
		assertEquals(readList.size(), 0);
	}

	@Test
	public void testListAfterUpdateWithCollectedStorageTermReadWithMatchingUppsalaFilter() {
		Filter filter = createFilterWithAPart("placeName", "Uppsala");

		createPlaceInStorageWithCollectedData(storageTerms, "nameInData");
		Collection<DataGroup> readList = recordStorage.readList(List.of("place"),
				filter).listOfDataGroups;
		assertEquals(readList.size(), 0);

		updatePlaceInStorageWithUppsalaStorageTerm();
		Collection<DataGroup> readList2 = recordStorage.readList(List.of("place"),
				filter).listOfDataGroups;
		assertEquals(readList2.size(), 1);
	}

	@Test
	public void testListAfterDeleteWithCollectedStorageTermReadWithMatchingUppsalaFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();

		Filter filter = createFilterWithAPart("placeName", "Uppsala");

		Collection<DataGroup> readList = recordStorage.readList(List.of("place"),
				filter).listOfDataGroups;
		assertEquals(readList.size(), 1);

		recordStorage.deleteByTypeAndId("place", "place:0001");
		Collection<DataGroup> readList2 = recordStorage.readList(List.of("place"),
				filter).listOfDataGroups;
		assertEquals(readList2.size(), 0);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithMatchingUppsalaFilterFromTwoRecords() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();
		createPlaceInStorageWithUppsalaStorageAndStockholmTerm();

		Filter filter = createFilterWithAPart("placeName", "Uppsala");

		StorageReadResult readList = recordStorage.readList(List.of("place"), filter);

		assertEquals(readList.start, 0);
		assertEquals(readList.totalNumberOfMatches, 2);
		assertEquals(readList.listOfDataGroups.size(), 2);

		dataCopierFactory.MCR.assertNumberOfCallsToMethod("factorForDataElement", 6);

		DataCopierSpy dcs4 = (DataCopierSpy) dataCopierFactory.MCR
				.getReturnValue("factorForDataElement", 4);
		DataCopierSpy dcs5 = (DataCopierSpy) dataCopierFactory.MCR
				.getReturnValue("factorForDataElement", 5);

		List<DataGroup> listOfDataGroups = readList.listOfDataGroups;
		dcs4.MCR.assertReturn("copy", 0, listOfDataGroups.get(0));
		dcs5.MCR.assertReturn("copy", 0, listOfDataGroups.get(1));
	}

	@Test
	public void testUpdateMultipleStorageTermsNoConcurrentException() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();
		createPlaceInStorageWithUppsalaStorageAndStockholmTerm();
		updatePlaceInStorageWithUppsalaStorageTerm();
		updatePlaceInStorageWithStockholmStorageTerm();

		Filter filter = createFilterWithAPart("placeName", "Uppsala");

		Collection<DataGroup> readList = recordStorage.readList(List.of("place"),
				filter).listOfDataGroups;
		assertEquals(readList.size(), 2);
	}

	private void createPlaceInStorageWithUppsalaStorageTerm(String nameInData) {
		Set<StorageTerm> storageTerms = createCollectedDataWithUppsalaStorageTerm();
		createPlaceInStorageWithCollectedData(storageTerms, nameInData);
	}

	private void updatePlaceInStorageWithUppsalaStorageTerm() {
		Set<StorageTerm> storageTerms = createCollectedDataWithUppsalaStorageTerm();
		updateUppsalaPlaceInStorageWithCollectedData(storageTerms);
	}

	private void updatePlaceInStorageWithStockholmStorageTerm() {
		Set<StorageTerm> storageTerms = createCollectedDataWithStockholmStorageTerm();
		updateStockholmPlaceInStorageWithCollectedData(storageTerms);
	}

	private Set<StorageTerm> createCollectedDataWithUppsalaStorageTerm() {
		StorageTerm storageTerm1 = new StorageTerm("placeNameStorageTerm", "placeName", "Uppsala");
		Set<StorageTerm> storageTerms = Set.of(storageTerm1);
		return storageTerms;
	}

	private void createPlaceInStorageWithCollectedData(Set<StorageTerm> storageTerms2,
			String nameInData) {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(nameInData,
						"place", "place:0001");
		recordStorage.create("place", "place:0001", dataGroup, storageTerms2, emptyLinkList,
				dataDivider);
	}

	private void updateUppsalaPlaceInStorageWithCollectedData(Set<StorageTerm> storageTerms) {
		DataGroup dataGroupOut = recordStorage.read(List.of("place"), "place:0001");
		recordStorage.update("place", "place:0001", dataGroupOut, storageTerms, emptyLinkList,
				dataDivider);
	}

	private void updateStockholmPlaceInStorageWithCollectedData(Set<StorageTerm> storageTerm) {
		DataGroup dataGroupOut = recordStorage.read(List.of("place"), "place:0002");
		recordStorage.update("place", "place:0002", dataGroupOut, storageTerm, emptyLinkList,
				dataDivider);
	}

	private void createPlaceInStorageWithStockholmStorageTerm() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"place", "place:0002");

		Set<StorageTerm> storageTerms = createCollectedDataWithStockholmStorageTerm();
		recordStorage.create("place", "place:0002", dataGroup, storageTerms, emptyLinkList,
				dataDivider);
	}

	private Set<StorageTerm> createCollectedDataWithStockholmStorageTerm() {
		StorageTerm storageTerm = new StorageTerm("placeNameStorageTerm", "placeName", "Stockholm");
		return Set.of(storageTerm);
	}

	private void createPlaceInStorageWithUppsalaStorageAndStockholmTerm() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"place", "place:0003");

		Set<StorageTerm> storageTerms = new LinkedHashSet<>();
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "placeName", "Uppsala"));
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "placeName", "Stockholm"));

		recordStorage.create("place", "place:0003", dataGroup, storageTerms, emptyLinkList,
				dataDivider);
	}

	@Test
	public void testReadRecordListNotFound() {
		String recordType = "place_NOT_FOUND";

		StorageReadResult readResult = recordStorage.readList(List.of(recordType), emptyFilter);

		assertEquals(readResult.listOfDataGroups.size(), 0);
	}

	@Test
	public void testReadRecordListEmptyFilter() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		StorageReadResult readList = recordStorage.readList(List.of("place"), emptyFilter);

		assertEquals(readList.start, 0);
		assertEquals(readList.totalNumberOfMatches, 2);
		assertEquals(readList.listOfDataGroups.size(), 2);
	}

	@Test
	public void testGetTotalNumberOfRecordsNoRecordsEmptyFilter() {
		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();

		long totalNumberOfRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(List.of("NOExistingRecords"), emptyFilter);
		assertEquals(totalNumberOfRecords, 0);

		assertFalse(termsHolder.findRecordsForFilterWasCalled);
	}

	@Test
	public void testGetTotalNumberOfRecordsNoRecordsWithFilter() {
		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();

		Filter filter = createFilterWithAPart("placeName", "Uppsala");

		long totalNumberOfRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(List.of("NOExistingRecords"), filter);
		assertEquals(totalNumberOfRecords, 0);
		assertFalse(termsHolder.findRecordsForFilterWasCalled);
	}

	@Test
	public void testGetTotalNumberOfRecordsWithRecordsEmptyFilter() {
		setUpStorageWithAuthorityRecords(3, 2);

		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();
		long totalNumberOfRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(List.of("organisation"), emptyFilter);
		assertEquals(totalNumberOfRecords, 2);

		assertFalse(termsHolder.findRecordsForFilterWasCalled);
	}

	@Test
	public void testGetTotalNumberOfRecordsWithRecordsWithFilter() {
		setUpStorageWithAuthorityRecords(3, 2);

		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();

		termsHolder.returnIdsForTypes.put("person", Arrays.asList("person:001"));

		Filter filter = createFilterWithAPart("personDomain", "uu");

		long totalNumberOfRecords = recordStorage.getTotalNumberOfRecordsForTypes(List.of("person"),
				filter);
		assertEquals(totalNumberOfRecords, 1);

		assertTrue(termsHolder.findRecordsForFilterWasCalled);

		assertEquals(termsHolder.type, "person");
		assertSame(termsHolder.filter, filter);
	}

	private void setUpStorageWithAuthorityRecords(int numOfPersons, int numOfOrgs) {
		Map<String, Map<String, DividerGroup>> records = setUpAuthorityRecords(numOfPersons,
				numOfOrgs);
		recordStorage = new RecordStorageInMemory(records);
	}

	@Test
	public void testGetTotalNumberOfRecordsWithRecordsWithFilterWithNoFromButTo() {
		setUpStorageWithAuthorityRecords(3, 2);
		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();

		setUpRecordsToReturnFromTermsHolder(termsHolder, "person", 16);
		Filter filter = createFilterWithAPart("placeName", "Uppsala");
		filter.toNo = 10;

		int asserterNumberReturned = 10;
		assertCorrectReturnedNumberOfRecords(termsHolder, filter, asserterNumberReturned);
		assertTrue(termsHolder.findRecordsForFilterWasCalled);
	}

	@Test
	public void testGetTotalNumberOfRecordsWithRecordsWithFilterWithFromNoTo() {
		setUpStorageWithAuthorityRecords(3, 2);

		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();
		setUpRecordsToReturnFromTermsHolder(termsHolder, "person", 16);

		Filter filter = createFilterWithAPart("placeName", "Uppsala");
		filter.fromNo = 10;

		int asserterNumberReturned = 7;
		assertCorrectReturnedNumberOfRecords(termsHolder, filter, asserterNumberReturned);
		assertTrue(termsHolder.findRecordsForFilterWasCalled);
	}

	@Test
	public void testGetTotalNumberOfRecordsWithRecordsWithFilterWithFromAndToNOPart() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<String, Map<String, DividerGroup>>();
		addRecords(records, "person", 23, "test");

		recordStorage = new RecordStorageInMemory(records);
		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();
		setUpRecordsToReturnFromTermsHolder(termsHolder, "person", 16);

		Filter filter = new Filter();
		filter.fromNo = 8;
		filter.toNo = 15;

		long totalNumberOfRecords = recordStorage.getTotalNumberOfRecordsForTypes(List.of("person"),
				filter);
		assertEquals(totalNumberOfRecords, 8);
		assertFalse(termsHolder.findRecordsForFilterWasCalled);
	}

	@Test
	public void testGetTotalNumberOfRecordsWithRecordsWithFilterWithFromAndToWhenToLargerThanSize() {
		setUpStorageWithAuthorityRecords(3, 2);

		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();
		setUpRecordsToReturnFromTermsHolder(termsHolder, "person", 16);

		Filter filter = createFilterWithAPart("placeName", "Uppsala");
		filter.fromNo = 8;
		filter.toNo = 35;

		int asserterNumberReturned = 9;
		assertCorrectReturnedNumberOfRecords(termsHolder, filter, asserterNumberReturned);
	}

	@Test
	public void testGetTotalNumberOfRecordsWithRecordsWithFilterWithFromWhenFromLargerThanSize() {
		setUpStorageWithAuthorityRecords(3, 2);

		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();
		setUpRecordsToReturnFromTermsHolder(termsHolder, "person", 16);
		Filter filter = createFilterWithAPart("placeName", "Uppsala");
		filter.fromNo = 18;

		int assertNumberReturned = 0;
		assertCorrectReturnedNumberOfRecords(termsHolder, filter, assertNumberReturned);
	}

	private void assertCorrectReturnedNumberOfRecords(CollectedTermsHolderSpy termsHolder,
			Filter filter, int assertNumberReturned) {
		long totalNumberOfRecords = recordStorage.getTotalNumberOfRecordsForTypes(List.of("person"),
				filter);
		assertEquals(totalNumberOfRecords, assertNumberReturned);

		assertEquals(termsHolder.type, "person");
		assertSame(termsHolder.filter, filter);
	}

	private void setUpRecordsToReturnFromTermsHolder(CollectedTermsHolderSpy termsHolder,
			String type, int numOfRecordsToReturn) {
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < numOfRecordsToReturn; i++) {
			ids.add(type + ":00" + i);
		}
		termsHolder.returnIdsForTypes.put(type, ids);
	}

	private CollectedTermsHolderSpy setUpCollectedTermsHolderSpy() {
		CollectedTermsHolderSpy termsHolder = new CollectedTermsHolderSpy();
		recordStorage.setCollectedTermsHolder(termsHolder);
		return termsHolder;
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsNoRecordsEmptyFilter() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<String, Map<String, DividerGroup>>();

		recordStorage = new RecordStorageInMemory(records);
		List<String> implementingTypes = new ArrayList<>(Arrays.asList("organisation", "person"));

		long totalNumberOfAbstractRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(implementingTypes, emptyFilter);
		assertEquals(totalNumberOfAbstractRecords, 0);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsEmptyFilter() {
		setUpStorageWithAuthorityRecords(3, 2);
		List<String> implementingTypes = new ArrayList<>(Arrays.asList("organisation", "person"));

		long totalNumberOfAbstractRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(implementingTypes, emptyFilter);
		assertEquals(totalNumberOfAbstractRecords, 5);
	}

	private Map<String, Map<String, DividerGroup>> setUpAuthorityRecords(int numOfPersons,
			int numOfOrgs) {
		Map<String, Map<String, DividerGroup>> records = new HashMap<String, Map<String, DividerGroup>>();
		addRecords(records, "person", numOfPersons, "test");
		addRecords(records, "organisation", numOfOrgs, "someSystemDivider");
		return records;
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsWithNoExistingImplementingTypesEmptyFilter() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<String, Map<String, DividerGroup>>();

		addOrganisations(records, 2);
		recordStorage = new RecordStorageInMemory(records);
		List<String> implementingTypes = new ArrayList<>(Arrays.asList("place"));

		long totalNumberOfAbstractRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(implementingTypes, emptyFilter);
		assertEquals(totalNumberOfAbstractRecords, 0);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsWithPartlyExistingImplementingTypesEmptyFilter() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<String, Map<String, DividerGroup>>();

		addOrganisations(records, 2);
		recordStorage = new RecordStorageInMemory(records);
		List<String> implementingTypes = new ArrayList<>(Arrays.asList("organisation", "person"));

		long totalNumberOfAbstractRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(implementingTypes, emptyFilter);
		assertEquals(totalNumberOfAbstractRecords, 2);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsNoRecordsWithFilter() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<String, Map<String, DividerGroup>>();

		recordStorage = new RecordStorageInMemory(records);
		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();

		Filter filter = createFilterWithAPart("personDomain", "uu");
		List<String> implementingTypes = new ArrayList<>(Arrays.asList("NOExistingRecords"));

		long totalNumberOfAbstractRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(implementingTypes, filter);
		assertEquals(totalNumberOfAbstractRecords, 0);
		assertFalse(termsHolder.findRecordsForFilterWasCalled);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsWithFilter() {
		setUpStorageWithAuthorityRecords(3, 2);

		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();
		termsHolder.returnIdsForTypes.put("organisation", Arrays.asList("organisation:001"));

		Filter filter = createFilterWithAPart("personDomain", "uu");

		List<String> implementingTypes = new ArrayList<>(Arrays.asList("person", "organisation"));

		long totalNumberOfAbstractRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(implementingTypes, filter);
		assertTrue(termsHolder.findRecordsForFilterWasCalled);
		assertEquals(totalNumberOfAbstractRecords, 1);
	}

	private void addOrganisations(Map<String, Map<String, DividerGroup>> records,
			int numOfRecords) {
		addRecords(records, "organisation", numOfRecords, "someSystemDivider");
	}

	private void addRecords(Map<String, Map<String, DividerGroup>> records, String type,
			int NumOfRecords, String dataDivider) {
		Map<String, DividerGroup> dividerForType = new HashMap<>();
		for (int i = 0; i < NumOfRecords; i++) {
			dividerForType.put(type + ":" + i,
					createDividerGroupWithDataDividerAndNameInData(dataDivider, type, i));
		}

		records.put(type, dividerForType);
	}

	private DividerGroup createDividerGroupWithDataDividerAndNameInData(String dataDivider,
			String nameInData, int index) {
		DataGroupOldSpy dataGroup = new DataGroupOldSpy(nameInData);
		dataGroup.addChild(new DataAtomicSpy("idForTest", "id" + index));
		return DividerGroup.withDataDividerAndDataGroup(dataDivider, dataGroup);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsWithFilterWithNoFromButTo() {
		setUpStorageCommonForAbstract();

		Filter filter = createFilterWithAPart("personDomain", "uu");
		filter.toNo = 11;

		List<String> implementingTypes = new ArrayList<>(Arrays.asList("person", "organisation"));

		long totalNumberOfAbstractRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(implementingTypes, filter);
		assertEquals(totalNumberOfAbstractRecords, 11);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsWithFilterWithFromNoTo() {
		setUpStorageCommonForAbstract();

		Filter filter = createFilterWithAPart("personDomain", "uu");
		filter.fromNo = 10;

		List<String> implementingTypes = new ArrayList<>(Arrays.asList("person", "organisation"));

		long totalNumberOfAbstractRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(implementingTypes, filter);
		assertEquals(totalNumberOfAbstractRecords, 9);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsWithFilterWithFromAndToNOPart() {
		setUpStorageCommonForAbstract();

		Filter filter = new Filter();
		filter.fromNo = 6;
		filter.toNo = 13;

		List<String> implementingTypes = new ArrayList<>(Arrays.asList("person", "organisation"));

		long totalNumberOfAbstractRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(implementingTypes, filter);
		assertEquals(totalNumberOfAbstractRecords, 8);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsWithFilterWithFromAndToWhenToLargerThanSize() {
		setUpStorageCommonForAbstract();

		Filter filter = createFilterWithAPart("placeName", "Uppsala");
		filter.fromNo = 8;
		filter.toNo = 35;

		List<String> implementingTypes = new ArrayList<>(Arrays.asList("person", "organisation"));
		long totalNumberOfAbstractRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(implementingTypes, filter);
		assertEquals(totalNumberOfAbstractRecords, 11);
	}

	private void setUpStorageCommonForAbstract() {
		setUpStorageWithAuthorityRecords(14, 12);
		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();
		setUpRecordsToReturnFromTermsHolder(termsHolder, "person", 10);
		setUpRecordsToReturnFromTermsHolder(termsHolder, "organisation", 8);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsWithFilterWithFromWhenFromLargerThanSize() {
		setUpStorageCommonForAbstract();

		Filter filter = createFilterWithAPart("placeName", "Uppsala");
		filter.fromNo = 20;

		List<String> implementingTypes = new ArrayList<>(Arrays.asList("person", "organisation"));
		long totalNumberOfAbstractRecords = recordStorage
				.getTotalNumberOfRecordsForTypes(implementingTypes, filter);
		assertEquals(totalNumberOfAbstractRecords, 0);
	}

	@Test
	public void testReadListOneTypeNoResult() throws Exception {
		OnlyForTestRecordStorageInMemory recordStorage = new OnlyForTestRecordStorageInMemory();

		String type = "someType";
		Filter filter = emptyFilter;
		recordStorage.readList(type, filter);

		recordStorage.MCR.assertParameterAsEqual("readList", 0, "types", List.of(type));
		recordStorage.MCR.assertParameter("readList", 0, "filter", filter);

		dataFactorySpy.MCR.assertMethodNotCalled("factorRecordGroupFromDataGroup");
	}

	@Test
	public void testReadListOneType() throws Exception {
		OnlyForTestRecordStorageInMemory recordStorage = new OnlyForTestRecordStorageInMemory();
		StorageReadResult storageReadResult = createStorageReadResultWithToDataGroups();
		recordStorage.MRV.setDefaultReturnValuesSupplier("readList", () -> storageReadResult);
		recordStorage.readList("someType", emptyFilter);

		dataFactorySpy.MCR.assertNumberOfCallsToMethod("factorRecordGroupFromDataGroup", 2);
		for (DataGroup dataGroup : storageReadResult.listOfDataGroups) {
			DataRecordGroup dataRecordGroup = (DataRecordGroup) dataFactorySpy.MCR
					.assertCalledParametersReturn("factorRecordGroupFromDataGroup", dataGroup);
			assertTrue(storageReadResult.listOfDataRecordGroups.contains(dataRecordGroup));
		}

		assertTrue(storageReadResult.listOfDataGroups.isEmpty());
	}

	private StorageReadResult createStorageReadResultWithToDataGroups() {
		StorageReadResult storageReadResult = new StorageReadResult();
		DataGroupSpy firstDataGroup = new DataGroupSpy();
		storageReadResult.listOfDataGroups.add(firstDataGroup);
		DataGroupSpy secondDataGroup = new DataGroupSpy();
		storageReadResult.listOfDataGroups.add(secondDataGroup);
		return storageReadResult;
	}

	private class OnlyForTestRecordStorageInMemory extends RecordStorageInMemory {
		public MethodCallRecorder MCR = new MethodCallRecorder();
		public MethodReturnValues MRV = new MethodReturnValues();

		public OnlyForTestRecordStorageInMemory() {
			MCR.useMRV(MRV);
			MRV.setDefaultReturnValuesSupplier("readList", StorageReadResult::new);
		}

		@Override
		public StorageReadResult readList(List<String> types, Filter filter) {
			return (StorageReadResult) MCR.addCallAndReturnFromMRV("types", types, "filter",
					filter);
		}
	}

	@Test
	public void testReadListNoFromOrTo() {
		createRecordStorageWithOrganisationRecords();
		Filter filter = setUpFilterForOrganisationWithFromTo("", "");

		StorageReadResult readList = recordStorage.readList(List.of("organisation"), filter);

		assertEquals(readList.listOfDataGroups.size(), 14);
	}

	private void createRecordStorageWithOrganisationRecords() {
		Map<String, Map<String, DividerGroup>> records = addOrganisationsToRecords();
		recordStorage = new RecordStorageInMemory(records);
	}

	@Test
	public void testReadListFromButNoTo() {
		createRecordStorageWithOrganisationRecords();
		Filter filter = setUpFilterForOrganisationWithFromTo("4", "");

		StorageReadResult readList = recordStorage.readList(List.of("organisation"), filter);

		assertEquals(readList.listOfDataGroups.size(), 11);
	}

	@Test
	public void testReadListNoFromButTo() {
		createRecordStorageWithOrganisationRecords();
		Filter filter = setUpFilterForOrganisationWithFromTo("", "7");

		StorageReadResult readList = recordStorage.readList(List.of("organisation"), filter);

		assertEquals(readList.listOfDataGroups.size(), 7);
	}

	@Test
	public void testReadListFromAndTo() {
		createRecordStorageWithOrganisationRecords();
		Filter filter = setUpFilterForOrganisationWithFromTo("2", "9");

		StorageReadResult readList = recordStorage.readList(List.of("organisation"), filter);

		assertEquals(readList.listOfDataGroups.size(), 8);
	}

	@Test
	public void testReadListFromAndToMinAndMaxNumber() {
		createRecordStorageWithOrganisationRecords();
		Filter filter = setUpFilterForOrganisationWithFromTo("1", "14");

		StorageReadResult readList = recordStorage.readList(List.of("organisation"), filter);

		assertEquals(readList.listOfDataGroups.size(), 14);
	}

	@Test
	public void testReadListFromAndToFirstRecord() {
		createRecordStorageWithOrganisationRecords();
		Filter filter = setUpFilterForOrganisationWithFromTo("1", "1");

		StorageReadResult readList = recordStorage.readList(List.of("organisation"), filter);

		assertEquals(readList.listOfDataGroups.size(), 1);
	}

	@Test
	public void testReadListFromAndToLastRecord() {
		createRecordStorageWithOrganisationRecords();
		Filter filter = setUpFilterForOrganisationWithFromTo("1", "1");

		StorageReadResult readList = recordStorage.readList(List.of("organisation"), filter);

		assertEquals(readList.listOfDataGroups.size(), 1);
	}

	@Test
	public void testReadListToGreaterThanNumberOfRecords() {
		createRecordStorageWithOrganisationRecords();
		Filter filter = setUpFilterForOrganisationWithFromTo("1", "18");

		StorageReadResult readList = recordStorage.readList(List.of("organisation"), filter);

		assertEquals(readList.listOfDataGroups.size(), 14);
	}

	private Filter setUpFilterForOrganisationWithFromTo(String fromNo, String toNo) {
		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();
		addIdsToReturnFromTermsHolder(termsHolder, 14, "organisation");

		Filter filter = createFilterWithAPart("organisationDomain", "uu");
		if (!fromNo.isEmpty()) {
			filter.fromNo = Long.valueOf(fromNo);
		}
		if (!toNo.isEmpty()) {
			filter.toNo = Long.valueOf(toNo);
		}
		return filter;
	}

	private Map<String, Map<String, DividerGroup>> addOrganisationsToRecords() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<String, Map<String, DividerGroup>>();

		addOrganisations(records, 28);

		addOrganisationRecordTypeToRecords(records);
		return records;
	}

	private void addOrganisationRecordTypeToRecords(
			Map<String, Map<String, DividerGroup>> records) {
		HashMap<String, DividerGroup> dividerForType = new HashMap<>();
		dividerForType.put("organisation", DividerGroup.withDataDividerAndDataGroup("test",
				new DataGroupOldSpy("organisation")));
		records.put("recordType", dividerForType);
	}

	private void addIdsToReturnFromTermsHolder(CollectedTermsHolderSpy termsHolder, int numToReturn,
			String type) {
		List<String> idsToReturn = new ArrayList<>();
		for (int i = 0; i < numToReturn; i++) {
			idsToReturn.add(type + ":" + i);
		}
		termsHolder.returnIdsForTypes.put(type, idsToReturn);
	}
}
