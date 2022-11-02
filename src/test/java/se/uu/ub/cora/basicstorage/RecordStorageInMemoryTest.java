/*
 * Copyright 2015, 2017, 2020 Uppsala University Library
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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.basicstorage.testdata.DataCreator;
import se.uu.ub.cora.basicstorage.testdata.TestDataRecordInMemoryStorage;
import se.uu.ub.cora.data.DataAtomic;
import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.collected.Link;
import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.data.copier.DataCopierProvider;
import se.uu.ub.cora.storage.RecordConflictException;
import se.uu.ub.cora.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.RecordStorage;

public class RecordStorageInMemoryTest {
	private static final String FROM_RECORD_TYPE = "fromRecordType";
	private static final String FROM_RECORD_ID = "fromRecordId";
	private static final String TO_RECORD_ID = "toRecordId";
	private static final String TO_RECORD_TYPE = "toRecordType";
	private RecordStorage recordStorage;
	private List<Link> emptyLinkList = DataCreator.createEmptyLinkList();
	private List<StorageTerm> storageTerms = Collections.emptyList();
	DataGroup emptyFilter = new DataGroupSpy("filter");
	private String dataDivider = "cora";
	// private DataGroupFactory dataGroupFactory;
	private DataCopierFactorySpy dataCopierFactory;

	@BeforeMethod
	public void beforeMethod() {
		// dataGroupFactory = new DataGroupFactorySpy();
		// DataGroupProvider.setDataGroupFactory(dataGroupFactory);
		dataCopierFactory = new DataCopierFactorySpy();
		DataCopierProvider.setDataCopierFactory(dataCopierFactory);

		recordStorage = new RecordStorageInMemory();
		DataGroup typeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("type", "true", "false");
		recordStorage.create("recordType", "type", typeRecordType, storageTerms, emptyLinkList,
				"cora");
		DataGroup recordTypeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("recordType", "true", "false");
		recordStorage.create("recordType", "recordType", recordTypeRecordType, storageTerms,
				emptyLinkList, "cora");

	}

	@Test
	public void testInitWithData() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<>();
		records.put("place", new HashMap<String, DividerGroup>());

		DataGroup dataGroup = createDataGroupWithRecordInfo();

		records.get("place").put("place:0001",
				DividerGroup.withDataDividerAndDataGroup(dataDivider, dataGroup));

		RecordStorageInMemory recordsInMemoryWithData = new RecordStorageInMemory(records);
		DataGroup placeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("place", "true", "false");
		recordsInMemoryWithData.create("recordType", "place", placeRecordType, storageTerms,
				emptyLinkList, "cora");
		assertEquals(recordsInMemoryWithData.read(List.of("place"), "place:0001"), dataGroup,
				"dataGroup should be the one added on startup");

	}

	private DataGroup createDataGroupWithRecordInfo() {
		return DataCreator.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
				"nameInData", "place", "place:0001");
	}

	// @Test
	// public void testCreateAndReadLinkList() {
	// DataGroup dataGroup = createDataGroupWithRecordInfo();
	// List<Link> linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);
	// recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
	// dataDivider);
	//
	// DataGroup readLinkList = recordStorage.readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);
	//
	// assertEquals(readLinkList.getChildren().size(), 2);
	// }

	@Test
	public void testGenerateTwoLinksPointingToRecordFromDifferentRecords() {
		createTwoLinksPointingToSameRecordFromDifferentRecords();

		Collection<DataGroup> generatedLinksPointingToRecord = recordStorage
				.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID);

		assertCorrectTwoLinksPointingToSameRecordFromDifferentRecords(
				generatedLinksPointingToRecord);
	}

	private void createTwoLinksPointingToSameRecordFromDifferentRecords() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<Link> linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);

		List<Link> linkList2 = createLinkListWithTwoLinks("fromRecordId2");
		recordStorage.create(FROM_RECORD_TYPE, "fromRecordId2", dataGroup, storageTerms, linkList2,
				dataDivider);
	}

	private void assertCorrectTwoLinksPointingToSameRecordFromDifferentRecords(
			Collection<DataGroup> generatedLinksPointToRecord) {
		assertEquals(generatedLinksPointToRecord.size(), 2);

		Iterator<DataGroup> generatedLinks = generatedLinksPointToRecord.iterator();
		assertRecordLinkIsCorrect(generatedLinks.next(), FROM_RECORD_TYPE, "fromRecordId2",
				TO_RECORD_TYPE, TO_RECORD_ID);
		assertRecordLinkIsCorrect(generatedLinks.next(), FROM_RECORD_TYPE, FROM_RECORD_ID,
				TO_RECORD_TYPE, TO_RECORD_ID);

		assertNoGeneratedLinksForRecordTypeAndRecordId(TO_RECORD_TYPE, "NOT_toRecordId");
		assertNoGeneratedLinksForRecordTypeAndRecordId("NOT_toRecordType", TO_RECORD_ID);
	}

	@Test
	public void testGenerateTwoLinksPointingToSameRecordFromSameRecord() {
		createTwoLinksPointingToSameRecordFromSameRecord();

		Collection<DataGroup> generatedLinksPointToRecord = recordStorage
				.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID);

		assertEquals(generatedLinksPointToRecord.size(), 2);

		assertCorrectTwoLinksPointingToSameRecordFromSameRecord(generatedLinksPointToRecord);
	}

	private void createTwoLinksPointingToSameRecordFromSameRecord() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<Link> linkList = createLinkListWithTwoLinksToSameRecord(FROM_RECORD_ID);
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);
	}

	private List<Link> createLinkListWithTwoLinksToSameRecord(String fromRecordId) {
		// DataGroup linkList = DataCreator.createEmptyLinkList();
		//
		// linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
		// TO_RECORD_TYPE, TO_RECORD_ID));
		//
		// linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
		// TO_RECORD_TYPE, TO_RECORD_ID));
		// return linkList;
		Link link1 = new Link(TO_RECORD_TYPE, TO_RECORD_ID);
		Link link2 = new Link(TO_RECORD_TYPE, TO_RECORD_ID);
		return List.of(link1, link2);
	}

	private void assertCorrectTwoLinksPointingToSameRecordFromSameRecord(
			Collection<DataGroup> generatedLinksPointToRecord) {
		Iterator<DataGroup> generatedLinks = generatedLinksPointToRecord.iterator();
		assertRecordLinkIsCorrect(generatedLinks.next(), FROM_RECORD_TYPE, FROM_RECORD_ID,
				TO_RECORD_TYPE, TO_RECORD_ID);
		assertRecordLinkIsCorrect(generatedLinks.next(), FROM_RECORD_TYPE, FROM_RECORD_ID,
				TO_RECORD_TYPE, TO_RECORD_ID);
	}

	private void assertRecordLinkIsCorrect(DataGroup recordToRecordLink, String fromRecordType,
			String fromRecordId, String toRecordType, String toRecordId) {
		assertEquals(recordToRecordLink.getNameInData(), "recordToRecordLink");

		DataGroup fromOut = recordToRecordLink.getFirstGroupWithNameInData("from");

		assertEquals(fromOut.getFirstAtomicValueWithNameInData("linkedRecordType"), fromRecordType);
		assertEquals(fromOut.getFirstAtomicValueWithNameInData("linkedRecordId"), fromRecordId);

		DataGroup toOut = recordToRecordLink.getFirstGroupWithNameInData("to");
		assertEquals(toOut.getFirstAtomicValueWithNameInData("linkedRecordType"), toRecordType);
		assertEquals(toOut.getFirstAtomicValueWithNameInData("linkedRecordId"), toRecordId);
	}

	private void assertNoGeneratedLinksForRecordTypeAndRecordId(String toRecordType,
			String toRecordId) {
		Collection<DataGroup> generatedLinksPointToRecord = recordStorage
				.getLinksToRecord(toRecordType, toRecordId);
		assertEquals(generatedLinksPointToRecord.size(), 0);
	}

	private List<Link> createLinkListWithTwoLinks(String fromRecordId) {
		// DataGroup linkList = DataCreator.createEmptyLinkList();
		//
		// linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
		// TO_RECORD_TYPE, TO_RECORD_ID));
		//
		// linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
		// TO_RECORD_TYPE, "toRecordId2"));
		// return linkList;
		Link link1 = new Link(TO_RECORD_TYPE, TO_RECORD_ID);
		Link link2 = new Link(TO_RECORD_TYPE, "toRecordId2");
		return List.of(link1, link2);
	}

	// @Test
	// public void testCreateWithoutLinkAndCollectedData() {
	// DataGroup dataGroup = createDataGroupWithRecordInfo();
	//
	// recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms,
	// emptyLinkList, dataDivider);
	//
	// DataGroup readLinkList = recordStorage.readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);
	// assertEquals(readLinkList.getChildren().size(), 0);
	// }

	@Test
	public void testCreateAndDeleteTwoWithoutLink() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms,
				emptyLinkList, dataDivider);
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID + "2", dataGroup, storageTerms,
				emptyLinkList, dataDivider);

		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID + "2");
	}

	@Test
	public void testDeletedDataGroupsIdCanBeUsedToStoreAnotherDataGroup() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"recordType", "recordId");

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms,
				emptyLinkList, dataDivider);
		// DataGroup readLinkList = recordStorage.readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);
		// assertEquals(readLinkList.getChildren().size(), 0);
		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData2",
						"recordType", "recordId");
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup2, storageTerms,
				emptyLinkList, dataDivider);
		// DataGroup readLinkList2 = recordStorage.readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);
		// assertEquals(readLinkList2.getChildren().size(), 0);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testInitWithemptyLinkList() {
		new RecordStorageInMemory(null);
	}

	private void createImageRecords() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
						"createNewWhenCopyingThisTopLevelGroup", "image", "image:0001");
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("image", "image:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData",
						"image", "image:0002");
		dataGroup2.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("image", "image:0002", dataGroup2, storageTerms, emptyLinkList,
				dataDivider);
	}

	private void createGenericBinaryRecord() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
						"createNewWhenCopyingThisTopLevelGroup", "genericBinary",
						"genericBinary:0001");
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("genericBinary", "genericBinary:0001", dataGroup, storageTerms,
				emptyLinkList, dataDivider);
	}

	@Test
	public void testReadRecordOfAbstractType() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createImageRecords();
		createGenericBinaryRecord();

		DataGroup image = recordStorage.read(List.of("image", "genericBinary"), "image:0001");
		assertNotNull(image);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testReadMissingRecordType() {
		recordStorage.read(List.of("nonExistingType"), "someId");
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testReadMissingRecordId() {
		RecordStorageInMemory recordsInMemoryWithTestData = TestDataRecordInMemoryStorage
				.createRecordStorageInMemoryWithTestData();
		recordsInMemoryWithTestData.read(List.of("place"), "");
	}

	@Test
	public void testCreateRead() {

		DataGroup dataGroup = createDataGroupWithRecordInfo();

		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);
		DataGroup dataGroupOut = recordStorage.read(List.of("type"), "place:0001");
		assertEquals(dataGroupOut.getNameInData(), dataGroup.getNameInData());
	}

	@Test
	public void testReadDataFromStorageShouldBeIndependentOfStoredData() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
						"createNewWhenCopyingThisTopLevelGroup", "place", "place:0001");
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		assertEquals(dataCopierFactory.numberOfFactoredCopiers, 3);
		DataGroup dataGroupOut = recordStorage.read(List.of("type"), "place:0001");
		assertEquals(dataCopierFactory.numberOfFactoredCopiers, 4);
		DataGroup dataGroupOut2 = recordStorage.read(List.of("type"), "place:0001");
		assertEquals(dataCopierFactory.numberOfFactoredCopiers, 5);

		assertNotSame(dataGroupOut, dataGroupOut2);
	}

	@Test
	public void testReadDataFromStorageShouldBeIndependentOfStoredDataAbstractType() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createImageRecords();
		createGenericBinaryRecord();

		assertEquals(dataCopierFactory.numberOfFactoredCopiers, 50);
		DataGroup image = recordStorage.read(List.of("genericBinary", "image"), "image:0001");
		assertEquals(dataCopierFactory.numberOfFactoredCopiers, 51);
		DataGroup image2 = recordStorage.read(List.of("genericBinary", "image"), "image:0001");
		assertEquals(dataCopierFactory.numberOfFactoredCopiers, 52);

		assertNotSame(image, image2);
	}

	@Test
	public void testCreateTworecordsRead() {

		DataGroup dataGroup = createDataGroupWithRecordInfo();

		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);
		recordStorage.create("type", "place:0002", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		DataGroup dataGroupOut = recordStorage.read(List.of("type"), "place:0001");
		assertEquals(dataGroupOut.getNameInData(), dataGroup.getNameInData());

		DataGroup dataGroupOut2 = recordStorage.read(List.of("type"), "place:0002");
		assertEquals(dataGroupOut2.getNameInData(), dataGroup.getNameInData());
	}

	@Test
	public void testCreateDataInStorageShouldBeIndependent() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		dataGroup.getChildren().clear();

		DataCopierSpy copier = (DataCopierSpy) dataCopierFactory.factoredCopier;
		assertSame(copier.originalDataElement, dataGroup);
	}

	@Test(expectedExceptions = RecordConflictException.class)
	public void testCreateConflict() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("type", "place1", dataGroup, storageTerms, emptyLinkList, dataDivider);
		recordStorage.create("type", "place1", dataGroup, storageTerms, emptyLinkList, dataDivider);
	}

	@Test
	public void testDelete() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);
		DataGroup dataGroupOut = recordStorage.read(List.of("type"), "place:0001");
		assertEquals(dataGroupOut.getNameInData(), dataGroup.getNameInData());

		recordStorage.deleteByTypeAndId("type", "place:0001");

		boolean recordFound = true;
		try {
			recordStorage.read(List.of("type"), "place:0001");
			recordFound = true;

		} catch (RecordNotFoundException e) {
			recordFound = false;
		}
		assertFalse(recordFound);
	}

	@Test
	public void testGenerateTwoLinksPointingToSameRecordFromSameRecordAndThenDeletingFromRecord() {
		createTwoLinksPointingToSameRecordFromSameRecord();

		Collection<DataGroup> generatedLinksPointToRecord = recordStorage
				.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID);

		assertEquals(generatedLinksPointToRecord.size(), 2);

		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
	}

	@Test
	public void testGenerateMoreLinksAndThenDeletingFromRecord() {
		createRecordFromOtherRecordIdWithLinkToToTypeAndOtherToRecordId();
		createTwoLinksPointingToSameRecordFromSameRecord();

		Collection<DataGroup> generatedLinksPointToRecord = recordStorage
				.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID);

		assertEquals(generatedLinksPointToRecord.size(), 2);

		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
	}

	private void createRecordFromOtherRecordIdWithLinkToToTypeAndOtherToRecordId() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		// List<Link> linkList = DataCreator.createEmptyLinkList();
		// linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE,
		// "fromOtherRecordId", TO_RECORD_TYPE, "toOtherRecordId"));
		Link link1 = new Link(TO_RECORD_TYPE, "toOtherRecordId");
		List<Link> linkList = List.of(link1);

		recordStorage.create(FROM_RECORD_TYPE, "fromOtherRecordId", dataGroup, storageTerms,
				linkList, dataDivider);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testLinkListIsRemovedOnDelete() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<Link> linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);

		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);

		((RecordStorageInMemory) recordStorage).readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);

	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testLinkListIsRemovedOnDeleteRecordTypeStillExistsInLinkListStorage() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<Link> linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms,
				createLinkListWithLinksForTestingRemoveOfLinks(), dataDivider);
		recordStorage.create(FROM_RECORD_TYPE, "fromRecordId2", dataGroup, storageTerms, linkList,
				dataDivider);

		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
		((RecordStorageInMemory) recordStorage).readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);

	}

	private List<Link> createLinkListWithLinksForTestingRemoveOfLinks() {
		// List<Link> linkList = DataCreator.createEmptyLinkList();
		//
		// linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, FROM_RECORD_ID,
		// TO_RECORD_TYPE, TO_RECORD_ID));
		// linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, "fromRecordId2",
		// TO_RECORD_TYPE, TO_RECORD_ID));
		// return linkList;
		Link link1 = new Link(TO_RECORD_TYPE, TO_RECORD_ID);
		Link link2 = new Link(TO_RECORD_TYPE, TO_RECORD_ID);
		return List.of(link1, link2);
	}

	@Test
	public void testGenerateLinksPointToRecordAreRemovedOnDelete() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<Link> linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 1);
		// delete
		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 0);

		assertFalse(recordStorage.linksExistForRecord(TO_RECORD_TYPE, TO_RECORD_ID));
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testDeleteNotFound() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);
		DataGroup dataGroupOut = recordStorage.read(List.of("type"), "place:0001");
		assertEquals(dataGroupOut.getNameInData(), dataGroup.getNameInData());

		recordStorage.deleteByTypeAndId("type", "place:0001_NOT_FOUND");
	}

	@Test
	public void testUpdate() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		DataGroup dataGroupOut = recordStorage.read(List.of("type"), "place:0001");
		DataAtomic child = (DataAtomic) dataGroupOut.getChildren().get(1);

		DataGroup dataGroup2 = createDataGroupWithRecordInfo();
		dataGroup2.addChild(new DataAtomicSpy("childId2", "childValue2"));
		recordStorage.update("type", "place:0001", dataGroup2, storageTerms, emptyLinkList,
				dataDivider);

		DataGroup dataGroupOut2 = recordStorage.read(List.of("type"), "place:0001");
		DataAtomic child2 = (DataAtomic) dataGroupOut2.getChildren().get(1);

		assertEquals(child.getValue(), "childValue");
		assertEquals(child2.getValue(), "childValue2");
	}

	@Test
	public void testUpdateWithoutLink() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		DataGroup dataGroup2 = createDataGroupWithRecordInfo();

		recordStorage.update("place", "place:0001", dataGroup2, storageTerms, emptyLinkList,
				dataDivider);

		// DataGroup readLinkList = recordStorage.readLinkList("place", "place:0001");
		// assertEquals(readLinkList.getChildren().size(), 0);
	}

	@Test
	public void testUpdateAndReadLinkList() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<Link> linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);

		// DataGroup readLinkList = recordStorage.readLinkList(FROM_RECORD_TYPE, FROM_RECORD_ID);

		// assertEquals(readLinkList.getChildren().size(), 2);

		// update
		List<Link> linkListOne = createLinkListWithOneLink(FROM_RECORD_ID);
		recordStorage.update(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkListOne,
				dataDivider);

		// DataGroup readLinkListUpdated = recordStorage.readLinkList(FROM_RECORD_TYPE,
		// FROM_RECORD_ID);
		//
		// assertEquals(readLinkListUpdated.getChildren().size(), 1);
	}

	private List<Link> createLinkListWithOneLink(String fromRecordId) {
		// DataGroup linkList = DataCreator.createEmptyLinkList();
		//
		// linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
		// TO_RECORD_TYPE, TO_RECORD_ID));
		//
		// return linkList;
		return List.of(new Link(TO_RECORD_TYPE, TO_RECORD_ID));
	}

	@Test
	public void testUpdateGenerateLinksPointToRecordAreRemovedAndAdded() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<Link> linkList = createLinkListWithTwoLinks(FROM_RECORD_ID);

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 1);
		// update
		recordStorage.update(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms,
				emptyLinkList, dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 0);

		// update
		recordStorage.update(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 1);
	}

	@Test
	public void testLinksFromSameRecordToSameRecordThanRemovingOne() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<Link> linkList = createLinkListWithThreeLinksTwoOfThemFromSameRecord(FROM_RECORD_ID);

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 3);
		// update
		linkList = createLinkListWithTwoLinksFromDifferentRecords(FROM_RECORD_ID);
		recordStorage.update(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 1);
	}

	private List<Link> createLinkListWithThreeLinksTwoOfThemFromSameRecord(String fromRecordId) {
		// DataGroup linkList = DataCreator.createEmptyLinkList();
		//
		// linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
		// TO_RECORD_TYPE, TO_RECORD_ID));
		//
		// linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
		// TO_RECORD_TYPE, TO_RECORD_ID));
		// linkList.addChild(DataCreator.createRecordToRecordLink("someOtherRecordType",
		// fromRecordId,
		// TO_RECORD_TYPE, TO_RECORD_ID));
		//
		// return linkList;
		Link link1 = new Link(TO_RECORD_TYPE, TO_RECORD_ID);
		Link link2 = new Link(TO_RECORD_TYPE, TO_RECORD_ID);
		Link link3 = new Link(TO_RECORD_TYPE, TO_RECORD_ID);
		return List.of(link1, link2, link3);
	}

	private List<Link> createLinkListWithTwoLinksFromDifferentRecords(String fromRecordId) {
		// DataGroup linkList = DataCreator.createEmptyLinkList();
		//
		// linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
		// TO_RECORD_TYPE, TO_RECORD_ID));
		//
		// linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
		// TO_RECORD_TYPE, "toRecordId2"));
		// return linkList;
		Link link1 = new Link(TO_RECORD_TYPE, TO_RECORD_ID);
		Link link2 = new Link(TO_RECORD_TYPE, "toRecordId2");
		return List.of(link1, link2);
	}

	private void assertNoOfLinksPointingToRecord(String toRecordType, String toRecordId,
			int expectedNoOfLinksPointingToRecord) {
		Collection<DataGroup> generatedLinksPointToRecord = recordStorage
				.getLinksToRecord(toRecordType, toRecordId);
		assertEquals(generatedLinksPointToRecord.size(), expectedNoOfLinksPointingToRecord);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testUpdateNotFoundType() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.update("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testUpdateNotFoundId() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);
		recordStorage.update("type", "place:0002", dataGroup, storageTerms, emptyLinkList,
				dataDivider);
	}

	@Test
	public void testUpdateDataInStorageShouldBeIndependent() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);
		recordStorage.update("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		dataGroup.getChildren().clear();

		DataCopierSpy copier = (DataCopierSpy) dataCopierFactory.factoredCopier;
		assertSame(copier.originalDataElement, dataGroup);
	}

	@Test
	public void testRecordExistForRecordTypeAndRecordId() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		assertTrue(recordStorage.recordExists(
				List.of("type"), "place:0001"));
	}

	@Test
	public void testRecordNOTExistForRecordTypeAndRecordIdMissingRecordId() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("place", "place:0004", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		assertFalse(recordStorage.recordExists(
				List.of("place"), "NOTplace:0001"));
	}

	@Test
	public void testRecordNOTExistForRecordTypeAndRecordIdMissingRecordType() {
		recordStorage = new RecordStorageInMemory();
		DataGroup recordTypeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("recordType", "true", "false");
		recordStorage.create("recordType", "recordType", recordTypeRecordType, storageTerms,
				emptyLinkList, "cora");

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		assertFalse(recordStorage.recordExists(
				List.of("NOTtype"), "place:0002"));
	}

	@Test
	public void testRecordExistForAbstractRecordTypeAndRecordId() {
		DataGroup abstractRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("abstractRecordType", "true",
						"true");
		recordStorage.create("recordType", "abstractRecordType", abstractRecordType, storageTerms,
				emptyLinkList, dataDivider);

		DataGroup implementingRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndParentId("implementingRecordType",
						"true", "abstractRecordType");
		recordStorage.create("recordType", "implementingRecordType", implementingRecordType,
				storageTerms, emptyLinkList, dataDivider);

		DataGroup otherImplementingRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndParentId("otherImplementingRecordType",
						"true", "abstractRecordType");
		recordStorage.create("recordType", "otherImplementingRecordType",
				otherImplementingRecordType, storageTerms, emptyLinkList, dataDivider);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("implementingRecordType", "someType:0001", dataGroup, storageTerms,
				emptyLinkList, dataDivider);

		assertTrue(recordStorage.recordExists(
				List.of("abstractRecordType", "implementingRecordType"), "someType:0001"));
	}

	@Test
	public void testRecordExistForAbstractRecordTypeAndRecordIdNoRecordsExists() {
		DataGroup abstractRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("abstractRecordType", "true",
						"true");
		recordStorage.create("recordType", "abstractRecordType", abstractRecordType, storageTerms,
				emptyLinkList, dataDivider);

		DataGroup otherImplementingRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndParentId("otherImplementingRecordType",
						"true", "abstractRecordType");
		recordStorage.create("recordType", "otherImplementingRecordType",
				otherImplementingRecordType, storageTerms, emptyLinkList, dataDivider);

		assertFalse(recordStorage.recordExists(
				List.of("abstractRecordType"), "someType:0001"));
	}

	@Test
	public void testRecordExistForNotAbstractNoRecordsExists() {
		DataGroup abstractRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("notAbstractRecordType", "true",
						"false");
		recordStorage.create("recordType", "notAbstractRecordType", abstractRecordType,
				storageTerms, emptyLinkList, dataDivider);

		assertFalse(recordStorage.recordExists(
				List.of("notAbstractRecordType"), "someType:0001"));
	}

	@Test
	public void testRecordNOTExistForAbstractRecordTypeAndRecordIdMissingRecordId() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("image", "image:0004", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		assertFalse(recordStorage.recordExists(
				List.of("binary"), "NOTimage:0004"));
	}

	@Test
	public void testRecordNOTExistForAbstractRecordTypeAndRecordIdRecordIdNoRecordTypeExist() {
		recordStorage = new RecordStorageInMemory();
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("image", "image:0004", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		assertFalse(recordStorage.recordExists(
				List.of("binary"), "NOTimage:0004"));
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testReadWhenRecordExistForAbstractRecordTypeAndRecordIdNoRecordsExists() {
		DataGroup abstractRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("abstractRecordType", "true",
						"true");
		recordStorage.create("recordType", "abstractRecordType", abstractRecordType, storageTerms,
				emptyLinkList, dataDivider);

		DataGroup otherImplementingRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndParentId("otherImplementingRecordType",
						"true", "abstractRecordType");
		recordStorage.create("recordType", "otherImplementingRecordType",
				otherImplementingRecordType, storageTerms, emptyLinkList, dataDivider);

		recordStorage.read(List.of("otherImplementingRecordType"), "someType:0001");
	}

}
