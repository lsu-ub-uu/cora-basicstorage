/*
 * Copyright 2015, 2017, 2018 Uppsala University Library
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.basicstorage.testdata.DataCreator;
import se.uu.ub.cora.basicstorage.testdata.TestDataRecordInMemoryStorage;
import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.DataGroupFactory;
import se.uu.ub.cora.data.DataGroupProvider;
import se.uu.ub.cora.data.copier.DataCopierFactory;
import se.uu.ub.cora.data.copier.DataCopierProvider;
import se.uu.ub.cora.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.StorageReadResult;

public class RecordStorageInMemoryListTest {
	private RecordStorageInMemory recordStorage;
	private DataGroup emptyLinkList = DataCreator.createEmptyLinkList();
	DataGroup emptyFilter = new DataGroupSpy("filter");
	private DataGroup emptyCollectedData = DataCreator.createEmptyCollectedData();
	private String dataDivider = "cora";
	private DataGroupFactory dataGroupFactory;
	private DataCopierFactory dataCopierFactory;

	@BeforeMethod
	public void beforeMethod() {
		dataGroupFactory = new DataGroupFactorySpy();
		DataGroupProvider.setDataGroupFactory(dataGroupFactory);
		dataCopierFactory = new DataCopierFactorySpy();
		DataCopierProvider.setDataCopierFactory(dataCopierFactory);

		recordStorage = new RecordStorageInMemory();
		DataGroup recordTypeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("place", "true", "false");
		recordStorage.create("recordType", "place", recordTypeRecordType,
				DataCreator.createEmptyCollectedData(), emptyLinkList, "cora");
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testListWithFilterButNoDataForTheType() {
		DataGroup filter = setUpFilterWithKeyAndValue("placeName", "Uppsala");

		recordStorage.readList("place", filter);
	}

	private DataGroup setUpFilterWithKeyAndValue(String key, String value) {
		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", key, value);
		filter.addChild(part);
		return filter;
	}

	@Test
	public void testListWithCollectedStorageTermReadWithEmptyFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();

		StorageReadResult readResult = recordStorage.readList("place", emptyFilter);
		Collection<DataGroup> readList = readResult.listOfDataGroups;

		assertEquals(readList.size(), 2);
		assertEquals(readResult.start, 0);
		assertEquals(readResult.totalNumberOfMatches, 2);
	}

	@Test
	public void testIndependentDataListWithCollectedStorageTermReadWithEmptyFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("createNewWhenCopyingThisTopLevelGroup");
		createPlaceInStorageWithStockholmStorageTerm();

		Collection<DataGroup> readList = recordStorage.readList("place",
				emptyFilter).listOfDataGroups;
		Collection<DataGroup> readList2 = recordStorage.readList("place",
				emptyFilter).listOfDataGroups;
		DataGroup first = readList.iterator().next();
		DataGroup secondRead = readList2.iterator().next();
		assertNotSame(first, secondRead);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithNonMatchingFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"NOT_UPPSALA");
		filter.addChild(part);

		StorageReadResult readResult = recordStorage.readList("place", filter);
		Collection<DataGroup> readList = readResult.listOfDataGroups;
		assertEquals(readList.size(), 0);
		assertEquals(readResult.start, 0);
		assertEquals(readResult.totalNumberOfMatches, 0);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithNonExisitingKeyFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0",
				"NOT_placeName", "Uppsala");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter).listOfDataGroups;
		assertEquals(readList.size(), 0);
	}

	@Test
	public void testListWithNoCollectedStorageTermReadWithFilter() {
		createPlaceInStorageWithCollectedData(emptyCollectedData, "nameInData");

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"Uppsala");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter).listOfDataGroups;
		assertEquals(readList.size(), 0);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithMatchingUppsalaFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"Uppsala");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter).listOfDataGroups;
		assertEquals(readList.size(), 1);
		DataGroup first = readList.iterator().next();
		assertEquals(first.getFirstGroupWithNameInData("recordInfo")
				.getFirstAtomicValueWithNameInData("id"), "place:0001");
	}

	@Test
	public void testIndependentDataListWithCollectedStorageTermReadWithMatchingUppsalaFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("createNewWhenCopyingThisTopLevelGroup");
		createPlaceInStorageWithStockholmStorageTerm();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"Uppsala");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter).listOfDataGroups;
		Collection<DataGroup> readList2 = recordStorage.readList("place", filter).listOfDataGroups;
		DataGroup first = readList.iterator().next();
		DataGroup secondRead = readList2.iterator().next();
		assertNotSame(first, secondRead);
	}

	@Test
	public void testListAfterUpdateWithNoCollectedStorageTermReadWithFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		updateUppsalaPlaceInStorageWithCollectedData(emptyCollectedData);

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"Uppsala");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter).listOfDataGroups;
		assertEquals(readList.size(), 0);
	}

	@Test
	public void testListAfterUpdateWithCollectedStorageTermReadWithMatchingUppsalaFilter() {
		DataGroup filter = setUpFilterWithKeyAndValue("placeName", "Uppsala");

		createPlaceInStorageWithCollectedData(emptyCollectedData, "nameInData");
		Collection<DataGroup> readList = recordStorage.readList("place", filter).listOfDataGroups;
		assertEquals(readList.size(), 0);

		updatePlaceInStorageWithUppsalaStorageTerm();
		Collection<DataGroup> readList2 = recordStorage.readList("place", filter).listOfDataGroups;
		assertEquals(readList2.size(), 1);
	}

	@Test
	public void testListAfterDeleteWithCollectedStorageTermReadWithMatchingUppsalaFilter() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"Uppsala");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter).listOfDataGroups;
		assertEquals(readList.size(), 1);

		recordStorage.deleteByTypeAndId("place", "place:0001");
		Collection<DataGroup> readList2 = recordStorage.readList("place", filter).listOfDataGroups;
		assertEquals(readList2.size(), 0);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithMatchingUppsalaFilterFromTwoRecords() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();
		createPlaceInStorageWithUppsalaStorageAndStockholmTerm();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"Uppsala");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter).listOfDataGroups;
		assertEquals(readList.size(), 2);
		Iterator<DataGroup> listIterator = readList.iterator();
		DataGroup first = listIterator.next();
		assertEquals(first.getFirstGroupWithNameInData("recordInfo")
				.getFirstAtomicValueWithNameInData("id"), "place:0001");
		DataGroup second = listIterator.next();
		assertEquals(second.getFirstGroupWithNameInData("recordInfo")
				.getFirstAtomicValueWithNameInData("id"), "place:0003");
	}

	@Test
	public void testUpdateMultipleStorageTermsNoConcurrentException() {
		createPlaceInStorageWithUppsalaStorageTerm("nameInData");
		createPlaceInStorageWithStockholmStorageTerm();
		createPlaceInStorageWithUppsalaStorageAndStockholmTerm();
		updatePlaceInStorageWithUppsalaStorageTerm();
		updatePlaceInStorageWithStockholmStorageTerm();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"Uppsala");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter).listOfDataGroups;
		assertEquals(readList.size(), 2);
	}

	private void createPlaceInStorageWithUppsalaStorageTerm(String nameInData) {
		DataGroup collectedData = createCollectedDataWithUppsalaStorageTerm();
		createPlaceInStorageWithCollectedData(collectedData, nameInData);
	}

	private void updatePlaceInStorageWithUppsalaStorageTerm() {
		DataGroup collectedData = createCollectedDataWithUppsalaStorageTerm();
		updateUppsalaPlaceInStorageWithCollectedData(collectedData);
	}

	private void updatePlaceInStorageWithStockholmStorageTerm() {
		DataGroup collectedData = createCollectedDataWithStockholmStorageTerm();
		updateStockholmPlaceInStorageWithCollectedData(collectedData);
	}

	private DataGroup createCollectedDataWithUppsalaStorageTerm() {
		DataGroup collectedData = DataCreator.createCollectedDataWithTypeAndId("place",
				"place:0001");
		DataGroup collectStorageTerm = new DataGroupSpy("storage");
		collectedData.addChild(collectStorageTerm);

		DataGroup collectedDataTerm = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"placeNameStorageTerm", "Uppsala", "placeName");
		collectStorageTerm.addChild(collectedDataTerm);
		return collectedData;
	}

	private void createPlaceInStorageWithCollectedData(DataGroup collectedData, String nameInData) {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(nameInData,
						"place", "place:0001");
		recordStorage.create("place", "place:0001", dataGroup, collectedData, emptyLinkList,
				dataDivider);
	}

	private void updateUppsalaPlaceInStorageWithCollectedData(DataGroup collectedData) {
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		recordStorage.update("place", "place:0001", dataGroupOut, collectedData, emptyLinkList,
				dataDivider);
	}

	private void updateStockholmPlaceInStorageWithCollectedData(DataGroup collectedData) {
		DataGroup dataGroupOut = recordStorage.read("place", "place:0002");
		recordStorage.update("place", "place:0002", dataGroupOut, collectedData, emptyLinkList,
				dataDivider);
	}

	private void createPlaceInStorageWithStockholmStorageTerm() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"place", "place:0002");

		DataGroup collectedData = createCollectedDataWithStockholmStorageTerm();

		recordStorage.create("place", "place:0002", dataGroup, collectedData, emptyLinkList,
				dataDivider);
	}

	private DataGroup createCollectedDataWithStockholmStorageTerm() {
		DataGroup collectedData = DataCreator.createCollectedDataWithTypeAndId("place",
				"place:0002");
		DataGroup collectStorageTerm = new DataGroupSpy("storage");
		collectedData.addChild(collectStorageTerm);

		DataGroup collectedDataTerm = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"placeNameStorageTerm", "Stockholm", "placeName");
		collectStorageTerm.addChild(collectedDataTerm);
		return collectedData;
	}

	private void createPlaceInStorageWithUppsalaStorageAndStockholmTerm() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"place", "place:0003");

		DataGroup collectedData = DataCreator.createCollectedDataWithTypeAndId("place",
				"place:0003");
		DataGroup collectStorageTerm = new DataGroupSpy("storage");
		collectedData.addChild(collectStorageTerm);

		DataGroup collectedDataTerm = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"placeNameStorageTerm", "Uppsala", "placeName");
		collectStorageTerm.addChild(collectedDataTerm);
		DataGroup collectedDataTerm2 = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"placeNameStorageTerm", "Stockholm", "placeName");
		collectStorageTerm.addChild(collectedDataTerm2);

		recordStorage.create("place", "place:0003", dataGroup, collectedData, emptyLinkList,
				dataDivider);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testReadRecordListNotFound() {
		String recordType = "place_NOT_FOUND";
		recordStorage.readList(recordType, emptyFilter);
	}

	@Test
	public void testReadRecordList() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();
		String recordType = "place";
		Collection<DataGroup> recordList = recordStorage.readList(recordType,
				emptyFilter).listOfDataGroups;
		assertEquals(recordList.iterator().next().getNameInData(), "authority");
	}

	@Test
	public void testReadAbstractRecordList() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createImageRecords("nameInData");
		createGenericBinaryRecord();

		String recordType = "binary";
		StorageReadResult readResult = recordStorage.readAbstractList(recordType, emptyFilter);
		Collection<DataGroup> recordList = readResult.listOfDataGroups;
		assertEquals(recordList.size(), 3);
		assertEquals(readResult.start, 0);
		assertEquals(readResult.totalNumberOfMatches, 3);
	}

	@Test
	public void testIndependentDataReadAbstractRecordList() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createImageRecords("createNewWhenCopyingThisTopLevelGroup");
		createGenericBinaryRecord("createNewWhenCopyingThisTopLevelGroup");

		// two and three have createNewWhenCopyingThisTopLevelGroup as nameInData
		Collection<DataGroup> readList = recordStorage.readAbstractList("binary",
				emptyFilter).listOfDataGroups;
		Collection<DataGroup> readList2 = recordStorage.readAbstractList("binary",
				emptyFilter).listOfDataGroups;
		Iterator<DataGroup> firstIterator = readList.iterator();
		firstIterator.next();
		DataGroup first2 = firstIterator.next();
		Iterator<DataGroup> secondIterator = readList2.iterator();
		secondIterator.next();
		DataGroup secondRead2 = secondIterator.next();
		assertNotSame(first2, secondRead2);
	}

	@Test
	public void testAbstractListWithCollectedStorageTermReadWithMatchingUppsalaFilter() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createImageRecords("nameInData");
		createGenericBinaryRecord("createNewWhenCopyingThisTopLevelGroup");

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "id",
				"image:0001");
		filter.addChild(part);

		// three have createNewWhenCopyingThisTopLevelGroup as nameInData
		Collection<DataGroup> readList = recordStorage.readAbstractList("binary",
				emptyFilter).listOfDataGroups;
		Collection<DataGroup> readList2 = recordStorage.readAbstractList("binary",
				emptyFilter).listOfDataGroups;
		Iterator<DataGroup> firstIterator = readList.iterator();
		firstIterator.next();
		firstIterator.next();
		DataGroup first3 = firstIterator.next();
		Iterator<DataGroup> secondIterator = readList2.iterator();
		secondIterator.next();
		secondIterator.next();
		DataGroup secondRead3 = secondIterator.next();
		assertNotSame(first3, secondRead3);
	}

	@Test
	public void testIndependentDataAbstractListWithCollectedStorageTermReadWithMatchingUppsalaFilter() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createImageRecords("nameInData");
		createGenericBinaryRecord();

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "id",
				"image:0001");
		filter.addChild(part);

		StorageReadResult readResult = recordStorage.readAbstractList("binary", filter);
		Collection<DataGroup> readList = readResult.listOfDataGroups;
		assertEquals(readList.size(), 1);
		assertEquals(readResult.totalNumberOfMatches, 1);
		DataGroup first = readList.iterator().next();
		assertEquals(first.getFirstGroupWithNameInData("recordInfo")
				.getFirstAtomicValueWithNameInData("id"), "image:0001");
	}

	private void createImageRecords(String nameInData) {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(nameInData,
						"image", "image:0001");
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));

		DataGroup collectedData = DataCreator.createCollectedDataWithTypeAndId("image",
				"image:0001");
		DataGroup collectStorageTerm = new DataGroupSpy("storage");
		collectedData.addChild(collectStorageTerm);

		DataGroup collectedDataTerm = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"idStorageTerm", "image:0001", "id");
		collectStorageTerm.addChild(collectedDataTerm);

		recordStorage.create("image", "image:0001", dataGroup, collectedData, emptyLinkList,
				dataDivider);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"image", "image:0002");
		dataGroup2.addChild(new DataAtomicSpy("childId", "childValue"));

		DataGroup collectedData2 = DataCreator.createCollectedDataWithTypeAndId("image",
				"image:0002");
		DataGroup collectStorageTerm2 = new DataGroupSpy("storage");
		collectedData2.addChild(collectStorageTerm2);

		DataGroup collectedDataTerm2 = DataCreator
				.createStorageTermWithRepeatIdAndTermIdAndTermValueAndStorageKey("1",
						"IdStorageTerm", "image:0002", "id");
		collectStorageTerm2.addChild(collectedDataTerm2);

		recordStorage.create("image", "image:0002", dataGroup2, collectedData2, emptyLinkList,
				dataDivider);
	}

	private void createGenericBinaryRecord() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"genericBinary", "genericBinary:0001");
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("genericBinary", "genericBinary:0001", dataGroup,
				DataCreator.createEmptyCollectedData(), emptyLinkList, dataDivider);
	}

	private void createGenericBinaryRecord(String nameInData) {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(nameInData,
						"genericBinary", "genericBinary:0001");
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("genericBinary", "genericBinary:0001", dataGroup,
				DataCreator.createEmptyCollectedData(), emptyLinkList, dataDivider);
	}

	@Test
	public void testReadAbstractRecordListOneImplementingChildHasNoRecords() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createImageRecords("nameInData");
		// create no records of genericBinary

		String recordType = "binary";
		Collection<DataGroup> recordList = recordStorage.readAbstractList(recordType,
				emptyFilter).listOfDataGroups;
		assertEquals(recordList.size(), 2);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testReadAbstractRecordListNoImplementingChildHasNoRecords() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		// create no records

		String recordType = "binary";
		recordStorage.readAbstractList(recordType, emptyFilter);
	}

	@Test
	public void testReadAbstractRecordListWithGrandChildren() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createChildOfAbstractAuthorityRecord();
		createGrandChildOfAbstractAuthorityRecord();

		String recordType = "abstractAuthority";
		Collection<DataGroup> recordList = recordStorage.readAbstractList(recordType,
				emptyFilter).listOfDataGroups;
		assertEquals(recordList.size(), 2);
	}

	private void createChildOfAbstractAuthorityRecord() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"childToAbstractAuthority", "childToAbstractAuthority:0001");
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("childToAbstractAuthority", "childToAbstractAuthority:0001", dataGroup,
				DataCreator.createEmptyCollectedData(), emptyLinkList, dataDivider);
	}

	private void createGrandChildOfAbstractAuthorityRecord() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"grandChildToAbstractAuthority", "grandChildToAbstractAuthority:0001");
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("grandChildToAbstractAuthority", "grandChildToAbstractAuthority:0001",
				dataGroup, DataCreator.createEmptyCollectedData(), emptyLinkList, dataDivider);
	}

	@Test
	public void testReadAbstractRecordListWithGrandChildrenNoRecordsForChild() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createGrandChildOfAbstractAuthorityRecord();

		String recordType = "abstractAuthority";
		Collection<DataGroup> recordList = recordStorage.readAbstractList(recordType,
				emptyFilter).listOfDataGroups;
		assertEquals(recordList.size(), 1);
	}

	@Test
	public void testReadAbstractRecordListWithGrandChildrenNoRecordsForGrandChild() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createChildOfAbstractAuthorityRecord();

		String recordType = "abstractAuthority";
		Collection<DataGroup> recordList = recordStorage.readAbstractList(recordType,
				emptyFilter).listOfDataGroups;
		assertEquals(recordList.size(), 1);
	}

	@Test
	public void testReadAbstractRecordListWithNonAbstractRecordTypeWithChildren() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createChildOfAbstractAuthorityRecord();
		createGrandChildOfAbstractAuthorityRecord();

		String recordType = "childToAbstractAuthority";
		Collection<DataGroup> recordList = recordStorage.readAbstractList(recordType,
				emptyFilter).listOfDataGroups;
		assertEquals(recordList.size(), 2);
	}

	@Test
	public void testGetTotalNumberOfRecordsNoRecordsEmptyFilter() {
		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();
		DataGroup filter = DataCreator.createEmptyFilter();

		long totalNumberOfRecords = recordStorage
				.getTotalNumberOfRecordsForType("NOExistingRecords", filter);
		assertEquals(totalNumberOfRecords, 0);

		assertFalse(termsHolder.findRecordsForFilterWasCalled);
	}

	@Test
	public void testGetTotalNumberOfRecordsNoRecordsWithFilter() {
		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();

		DataGroup filter = setUpFilterWithKeyAndValue("placeName", "Uppsala");

		long totalNumberOfRecords = recordStorage
				.getTotalNumberOfRecordsForType("NOExistingRecords", filter);
		assertEquals(totalNumberOfRecords, 0);
		assertFalse(termsHolder.findRecordsForFilterWasCalled);
	}

	@Test
	public void testGetTotalNumberOfRecordsWithRecordsEmptyFilter() {
		Map<String, Map<String, DividerGroup>> records = setUpAuthorityRecords();
		recordStorage = new RecordStorageInMemory(records);

		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();
		long totalNumberOfRecords = recordStorage.getTotalNumberOfRecordsForType("organisation",
				DataCreator.createEmptyFilter());
		assertEquals(totalNumberOfRecords, 2);

		assertFalse(termsHolder.findRecordsForFilterWasCalled);
	}

	@Test
	public void testGetTotalNumberOfRecordsWithRecordsWithFilter() {
		Map<String, Map<String, DividerGroup>> records = setUpAuthorityRecords();
		recordStorage = new RecordStorageInMemory(records);

		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();

		termsHolder.returnIdsForTypes.put("person", Arrays.asList("person:001"));

		DataGroup filter = setUpFilterWithKeyAndValue("personDomain", "uu");

		long totalNumberOfRecords = recordStorage.getTotalNumberOfRecordsForType("person", filter);
		assertEquals(totalNumberOfRecords, 1);

		assertTrue(termsHolder.findRecordsForFilterWasCalled);
		assertEquals(totalNumberOfRecords, 1);

		assertEquals(termsHolder.type, "person");
		assertSame(termsHolder.filter, filter);
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

		long totalNumberOfAbstractRecords = recordStorage.getTotalNumberOfRecordsForAbstractType(
				"authorityNOTUsedInThisImplementation", implementingTypes, emptyFilter);
		assertEquals(totalNumberOfAbstractRecords, 0);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsEmptyFilter() {
		Map<String, Map<String, DividerGroup>> records = setUpAuthorityRecords();
		recordStorage = new RecordStorageInMemory(records);
		List<String> implementingTypes = new ArrayList<>(Arrays.asList("organisation", "person"));

		long totalNumberOfAbstractRecords = recordStorage.getTotalNumberOfRecordsForAbstractType(
				"authorityNOTUsedInThisImplementation", implementingTypes, emptyFilter);
		assertEquals(totalNumberOfAbstractRecords, 5);
	}

	private Map<String, Map<String, DividerGroup>> setUpAuthorityRecords() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<String, Map<String, DividerGroup>>();

		addPersons(records);
		addOrganisations(records, 2);
		return records;
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsWithNoExistingImplementingTypesEmptyFilter() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<String, Map<String, DividerGroup>>();

		addOrganisations(records, 2);
		recordStorage = new RecordStorageInMemory(records);
		List<String> implementingTypes = new ArrayList<>(Arrays.asList("place"));

		long totalNumberOfAbstractRecords = recordStorage.getTotalNumberOfRecordsForAbstractType(
				"authorityNOTUsedInThisImplementation", implementingTypes, emptyFilter);
		assertEquals(totalNumberOfAbstractRecords, 0);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsWithPartlyExistingImplementingTypesEmptyFilter() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<String, Map<String, DividerGroup>>();

		addOrganisations(records, 2);
		recordStorage = new RecordStorageInMemory(records);
		List<String> implementingTypes = new ArrayList<>(Arrays.asList("organisation", "person"));

		long totalNumberOfAbstractRecords = recordStorage.getTotalNumberOfRecordsForAbstractType(
				"authorityNOTUsedInThisImplementation", implementingTypes, emptyFilter);
		assertEquals(totalNumberOfAbstractRecords, 2);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsNoRecordsWithFilter() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<String, Map<String, DividerGroup>>();

		recordStorage = new RecordStorageInMemory(records);
		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();

		DataGroup filter = setUpFilterWithKeyAndValue("personDomain", "uu");
		List<String> implementingTypes = new ArrayList<>(Arrays.asList("NOExistingRecords"));

		long totalNumberOfAbstractRecords = recordStorage.getTotalNumberOfRecordsForAbstractType(
				"authorityNOTUsedInThisImplementation", implementingTypes, filter);
		assertEquals(totalNumberOfAbstractRecords, 0);
		assertFalse(termsHolder.findRecordsForFilterWasCalled);
	}

	@Test
	public void testGetTotalNumberOfAbstractRecordsWithRecordsWithFilter() {
		Map<String, Map<String, DividerGroup>> records = setUpAuthorityRecords();
		recordStorage = new RecordStorageInMemory(records);

		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();
		termsHolder.returnIdsForTypes.put("organisation", Arrays.asList("organisation:001"));

		DataGroup filter = setUpFilterWithKeyAndValue("personDomain", "uu");

		List<String> implementingTypes = new ArrayList<>(Arrays.asList("person", "organisation"));

		long totalNumberOfAbstractRecords = recordStorage.getTotalNumberOfRecordsForAbstractType(
				"authorityNOTUsedInThisImplementation", implementingTypes, filter);
		assertTrue(termsHolder.findRecordsForFilterWasCalled);
		assertEquals(totalNumberOfAbstractRecords, 1);
	}

	private void addPersons(Map<String, Map<String, DividerGroup>> records) {
		addRecords(records, "person", 3, "test");
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
		DataGroupSpy dataGroup = new DataGroupSpy(nameInData);
		dataGroup.addChild(new DataAtomicSpy("idForTest", "id" + index));
		return DividerGroup.withDataDividerAndDataGroup(dataDivider, dataGroup);
	}

	/**************************************************/
	@Test
	public void testReadListNoFromOrTo() {
		createRecordStorageWithOrganisationRecords();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("", "");

		StorageReadResult readList = recordStorage.readList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 14);
	}

	private void createRecordStorageWithOrganisationRecords() {
		Map<String, Map<String, DividerGroup>> records = addOrganisationsToRecords();
		recordStorage = new RecordStorageInMemory(records);
	}

	@Test
	public void testReadListFromButNoTo() {
		createRecordStorageWithOrganisationRecords();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("4", "");

		StorageReadResult readList = recordStorage.readList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 11);
	}

	@Test
	public void testReadListNoFromButTo() {
		createRecordStorageWithOrganisationRecords();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("", "7");

		StorageReadResult readList = recordStorage.readList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 7);
	}

	@Test
	public void testReadListFromAndTo() {
		createRecordStorageWithOrganisationRecords();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("2", "9");

		StorageReadResult readList = recordStorage.readList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 8);
	}

	@Test
	public void testReadListFromAndToMinAndMaxNumber() {
		createRecordStorageWithOrganisationRecords();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("1", "14");

		StorageReadResult readList = recordStorage.readList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 14);
	}

	@Test
	public void testReadListFromAndToFirstRecord() {
		createRecordStorageWithOrganisationRecords();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("1", "1");

		StorageReadResult readList = recordStorage.readList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 1);
	}

	@Test
	public void testReadListFromAndToLastRecord() {
		createRecordStorageWithOrganisationRecords();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("1", "1");

		StorageReadResult readList = recordStorage.readList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 1);
	}

	@Test
	public void testReadListToGreaterThanNumberOfRecords() {
		createRecordStorageWithOrganisationRecords();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("1", "18");

		StorageReadResult readList = recordStorage.readList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 14);
	}

	private DataGroup setUpFilterForOrganisationWithFromTo(String fromNo, String toNo) {
		CollectedTermsHolderSpy termsHolder = setUpCollectedTermsHolderSpy();
		addIdsToReturnFromTermsHolder(termsHolder, 14, "organisation");

		DataGroup filter = setUpFilterWithKeyAndValue("organisationDomain", "uu");
		if (!fromNo.isEmpty()) {
			filter.addChild(new DataAtomicSpy("fromNo", fromNo));
		}
		if (!toNo.isEmpty()) {
			filter.addChild(new DataAtomicSpy("toNo", toNo));
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
		dividerForType.put("organisation",
				DividerGroup.withDataDividerAndDataGroup("test", new DataGroupSpy("organisation")));
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

	@Test
	public void testReadAbstractListNoFromOrTo() {
		createRecordStorageWithOrganisationRecordsAbstract();

		DataGroup filter = setUpFilterForOrganisationWithFromTo("", "");

		StorageReadResult readList = recordStorage.readAbstractList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 14);
	}

	@Test
	public void testReadAbstractListFromButNoTo() {
		createRecordStorageWithOrganisationRecordsAbstract();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("4", "");

		StorageReadResult readList = recordStorage.readAbstractList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 11);
	}

	@Test
	public void testReadAbstractListNoFromButTo() {
		createRecordStorageWithOrganisationRecordsAbstract();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("", "7");

		StorageReadResult readList = recordStorage.readAbstractList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 7);
	}

	@Test
	public void testReadAbstractListFromAndTo() {
		createRecordStorageWithOrganisationRecordsAbstract();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("2", "9");

		StorageReadResult readList = recordStorage.readAbstractList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 8);
	}

	@Test
	public void testReadAbstractListFromAndToMinAndMaxNumber() {
		createRecordStorageWithOrganisationRecordsAbstract();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("1", "14");

		StorageReadResult readList = recordStorage.readAbstractList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 14);
	}

	@Test
	public void testReadAbstractListFromAndToFirstRecord() {
		createRecordStorageWithOrganisationRecordsAbstract();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("1", "1");

		StorageReadResult readList = recordStorage.readAbstractList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 1);
	}

	@Test
	public void testReadAbstractListFromAndToLastRecord() {
		createRecordStorageWithOrganisationRecordsAbstract();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("1", "1");

		StorageReadResult readList = recordStorage.readAbstractList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 1);
	}

	@Test
	public void testReadAbstractListToGreaterThanNumberOfRecords() {
		createRecordStorageWithOrganisationRecordsAbstract();
		DataGroup filter = setUpFilterForOrganisationWithFromTo("1", "18");

		StorageReadResult readList = recordStorage.readAbstractList("organisation", filter);

		assertEquals(readList.listOfDataGroups.size(), 14);
	}

	private void createRecordStorageWithOrganisationRecordsAbstract() {
		Map<String, Map<String, DividerGroup>> records = addOrganisationsToRecords();
		addRecordTypeRecordTypeToRecords(records);
		recordStorage = new RecordStorageInMemory(records);
	}

	private void addRecordTypeRecordTypeToRecords(Map<String, Map<String, DividerGroup>> records) {
		Map<String, DividerGroup> recordTypes = records.get("recordType");
		recordTypes.put("recordType",
				DividerGroup.withDataDividerAndDataGroup("test", new DataGroupSpy("recordType")));
	}

}
