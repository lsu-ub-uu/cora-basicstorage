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
import static org.testng.Assert.assertTrue;

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
import se.uu.ub.cora.storage.RecordConflictException;
import se.uu.ub.cora.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.RecordStorage;

public class RecordStorageInMemoryTest {
	private static final String FROM_RECORD_TYPE = "fromRecordType";
	private static final String FROM_RECORD_ID = "fromRecordId";
	private static final String TO_RECORD_ID = "toRecordId";
	private static final String TO_RECORD_TYPE = "toRecordType";
	private static final int NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD = 2;
	private RecordStorage recordStorage;
	private Set<Link> emptyLinkList = Collections.emptySet();
	private Set<StorageTerm> storageTerms = Collections.emptySet();
	DataGroup emptyFilter = new DataGroupSpy("filter");
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
		DataGroup read = recordsInMemoryWithData.read(List.of("place"), "place:0001");

		dataCopierFactory.MCR.assertNumberOfCallsToMethod("factorForDataElement",
				NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD + 2);
		dataCopierFactory.MCR.assertParameters("factorForDataElement",
				NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD, placeRecordType);
		DataCopierSpy dataCopier = (DataCopierSpy) dataCopierFactory.MCR
				.getReturnValue("factorForDataElement", NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD + 1);
		dataCopier.MCR.assertReturn("copy", 0, read);

	}

	private DataGroup createDataGroupWithRecordInfo() {
		return DataCreator.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
				"nameInData", "place", "place:0001");
	}

	@Test
	public void testGenerateTwoLinksPointingToRecordFromDifferentRecords() {
		createTwoLinksPointingToSameRecordFromDifferentRecords();

		Collection<Link> generatedLinksPointingToRecord = recordStorage
				.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID);

		assertCorrectTwoLinksPointingToSameRecordFromDifferentRecords(
				generatedLinksPointingToRecord);
	}

	private void createTwoLinksPointingToSameRecordFromDifferentRecords() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		Set<Link> linkList = createLinkListWithTwoLinks();
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);

		Set<Link> linkList2 = createLinkListWithTwoLinks();
		recordStorage.create(FROM_RECORD_TYPE, "fromRecordId2", dataGroup, storageTerms, linkList2,
				dataDivider);
	}

	private void assertCorrectTwoLinksPointingToSameRecordFromDifferentRecords(
			Collection<Link> generatedLinksPointingToRecord) {
		assertEquals(generatedLinksPointingToRecord.size(), 2);

		Link l1 = new Link(FROM_RECORD_TYPE, FROM_RECORD_ID);
		Link l2 = new Link(FROM_RECORD_TYPE, "fromRecordId2");
		generatedLinksPointingToRecord.containsAll(Set.of(l1, l2));

		assertNoGeneratedLinksForRecordTypeAndRecordId(TO_RECORD_TYPE, "NOT_toRecordId");
		assertNoGeneratedLinksForRecordTypeAndRecordId("NOT_toRecordType", TO_RECORD_ID);
	}

	private void createTwoLinksPointingToSameRecordFromSameRecord() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		Set<Link> linkList = createLinkListWithTwoLinksToSameRecord();
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);
	}

	private Set<Link> createLinkListWithTwoLinksToSameRecord() {
		Set<Link> links = new LinkedHashSet<>();
		links.add(new Link(TO_RECORD_TYPE, TO_RECORD_ID));
		links.add(new Link(TO_RECORD_TYPE, TO_RECORD_ID));
		return links;
	}

	private void assertNoGeneratedLinksForRecordTypeAndRecordId(String toRecordType,
			String toRecordId) {
		Collection<Link> generatedLinksPointToRecord = recordStorage.getLinksToRecord(toRecordType,
				toRecordId);
		assertEquals(generatedLinksPointToRecord.size(), 0);
	}

	private Set<Link> createLinkListWithTwoLinks() {
		Set<Link> links = new LinkedHashSet<>();
		links.add(new Link(TO_RECORD_TYPE, TO_RECORD_ID));
		links.add(new Link(TO_RECORD_TYPE, "toRecordId2"));
		return links;
	}

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
		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("nameInData2",
						"recordType", "recordId");
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup2, storageTerms,
				emptyLinkList, dataDivider);
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

	@Test(expectedExceptions = RecordNotFoundException.class, expectedExceptionsMessageRegExp = ""
			+ "No record exists with recordId: nonExistingRecordId")
	public void testCallReadMissingRecordId() throws Exception {
		RecordStorageInMemory recordsInMemoryWithTestData = TestDataRecordInMemoryStorage
				.createRecordStorageInMemoryWithTestData();
		recordsInMemoryWithTestData.read("place", "nonExistingRecordId");
	}

	@Test(expectedExceptions = RecordNotFoundException.class, expectedExceptionsMessageRegExp = ""
			+ "No record exists with recordType: nonExistingRecordType")
	public void testCallReadMissingRecordType() throws Exception {
		RecordStorageInMemory recordsInMemoryWithTestData = TestDataRecordInMemoryStorage
				.createRecordStorageInMemoryWithTestData();
		recordsInMemoryWithTestData.read("nonExistingRecordType", "someId");
	}

	@Test
	public void testRead() {
		Map<String, Map<String, DividerGroup>> records = new HashMap<>();
		records.put("type", new HashMap<String, DividerGroup>());
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		records.get("type").put("id:0001",
				DividerGroup.withDataDividerAndDataGroup(dataDivider, dataGroup));
		RecordStorageInMemory recordsInMemoryWithData = new RecordStorageInMemory(records);

		DataRecordGroup dataRecordGroupOut = recordsInMemoryWithData.read("type", "id:0001");

		dataFactorySpy.MCR.assertReturn("factorRecordGroupFromDataGroup", 0, dataRecordGroupOut);
		dataCopierFactory.MCR.assertParameters("factorForDataElement",
				NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD, dataGroup);
		DataCopierSpy dataCopier = (DataCopierSpy) dataCopierFactory.MCR
				.getReturnValue("factorForDataElement", NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD);
		var copiedDataGroup = dataCopier.MCR.getReturnValue("copy", 0);
		dataFactorySpy.MCR.assertParameter("factorRecordGroupFromDataGroup", 0, "dataGroup",
				copiedDataGroup);
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

		dataCopierFactory.MCR.assertParameters("factorForDataElement",
				NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD, dataGroup);
		DataCopierSpy dataCopier = (DataCopierSpy) dataCopierFactory.MCR
				.getReturnValue("factorForDataElement", NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD + 1);
		dataCopier.MCR.assertReturn("copy", 0, dataGroupOut);
	}

	@Test
	public void testReadDataFromStorageShouldBeIndependentOfStoredData() {
		DataGroup dataGroup = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
						"createNewWhenCopyingThisTopLevelGroup", "place", "place:0001");
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		dataCopierFactory.MCR.assertNumberOfCallsToMethod("factorForDataElement", 3);
		DataGroup dataGroupOut = recordStorage.read(List.of("type"), "place:0001");
		dataCopierFactory.MCR.assertNumberOfCallsToMethod("factorForDataElement", 4);
		DataGroup dataGroupOut2 = recordStorage.read(List.of("type"), "place:0001");
		dataCopierFactory.MCR.assertNumberOfCallsToMethod("factorForDataElement", 5);

		assertNotSame(dataGroupOut, dataGroupOut2);
	}

	@Test
	public void testReadDataFromStorageShouldBeIndependentOfStoredDataAbstractType() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		createImageRecords();
		createGenericBinaryRecord();

		// assertEquals(dataCopierFactory.numberOfFactoredCopiers, 49);
		dataCopierFactory.MCR.assertNumberOfCallsToMethod("factorForDataElement", 49);
		DataGroup image = recordStorage.read(List.of("genericBinary", "image"), "image:0001");
		// assertEquals(dataCopierFactory.numberOfFactoredCopiers, 50);
		dataCopierFactory.MCR.assertNumberOfCallsToMethod("factorForDataElement", 50);
		DataGroup image2 = recordStorage.read(List.of("genericBinary", "image"), "image:0001");
		// assertEquals(dataCopierFactory.numberOfFactoredCopiers, 51);
		dataCopierFactory.MCR.assertNumberOfCallsToMethod("factorForDataElement", 51);

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
		dataCopierFactory.MCR.assertParameters("factorForDataElement",
				NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD + 1, dataGroup);
		DataCopierSpy dataCopier = (DataCopierSpy) dataCopierFactory.MCR
				.getReturnValue("factorForDataElement", NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD + 2);
		dataCopier.MCR.assertReturn("copy", 0, dataGroupOut);

		DataGroup dataGroupOut2 = recordStorage.read(List.of("type"), "place:0002");
		dataCopierFactory.MCR.assertParameters("factorForDataElement",
				NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD, dataGroup);
		DataCopierSpy dataCopier2 = (DataCopierSpy) dataCopierFactory.MCR
				.getReturnValue("factorForDataElement", NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD + 3);
		dataCopier2.MCR.assertReturn("copy", 0, dataGroupOut2);
	}

	@Test
	public void testCreateDataInStorageShouldBeIndependent() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		dataGroup.getChildren().clear();

		dataCopierFactory.MCR.assertParameters("factorForDataElement", 2, dataGroup);
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

		Collection<Link> generatedLinksPointToRecord = recordStorage
				.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID);

		assertEquals(generatedLinksPointToRecord.size(), 1);

		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
	}

	@Test
	public void testGenerateMoreLinksAndThenDeletingFromRecord() {
		createRecordFromOtherRecordIdWithLinkToToTypeAndOtherToRecordId();
		createTwoLinksPointingToSameRecordFromSameRecord();

		Collection<Link> generatedLinksPointToRecord = recordStorage
				.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID);

		assertEquals(generatedLinksPointToRecord.size(), 1);

		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
	}

	private void createRecordFromOtherRecordIdWithLinkToToTypeAndOtherToRecordId() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		Link link1 = new Link(TO_RECORD_TYPE, "toOtherRecordId");
		Set<Link> linkList = Set.of(link1);

		recordStorage.create(FROM_RECORD_TYPE, "fromOtherRecordId", dataGroup, storageTerms,
				linkList, dataDivider);
	}

	@Test
	public void testLinkListIsRemovedOnDelete() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		Set<Link> linkList = createLinkListWithTwoLinks();

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);

		assertFalse(recordStorage.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID).isEmpty());
		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);

		assertTrue(recordStorage.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID).isEmpty());
	}

	@Test
	public void testTwoRecordsPointsToSameRecordAfterDeleteOnlyOneStillThere() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		Set<Link> linkList = createLinkListWithTwoLinks();

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms,
				createLinkListWithTwoLinksToSameRecord(), dataDivider);
		recordStorage.create(FROM_RECORD_TYPE, "fromRecordId2", dataGroup, storageTerms, linkList,
				dataDivider);

		assertEquals(recordStorage.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID).size(), 2);
		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);

		assertEquals(recordStorage.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID).size(), 1);
		assertFalse(recordStorage.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID).isEmpty());
	}

	// private List<Link> createLinkListWithLinksForTestingRemoveOfLinks() {
	// Link link1 = new Link(TO_RECORD_TYPE, TO_RECORD_ID);
	// Link link2 = new Link(TO_RECORD_TYPE, TO_RECORD_ID);
	// return List.of(link1, link2);
	// }

	@Test
	public void testGenerateLinksPointToRecordAreRemovedOnDelete() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		Set<Link> linkList = createLinkListWithTwoLinks();

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 1);
		// delete
		recordStorage.deleteByTypeAndId(FROM_RECORD_TYPE, FROM_RECORD_ID);
		// assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 0);

		assertEquals(recordStorage.getLinksToRecord(TO_RECORD_TYPE, TO_RECORD_ID).size(), 0);
		assertFalse(recordStorage.linksExistForRecord(TO_RECORD_TYPE, TO_RECORD_ID));
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testDeleteNotFound() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		recordStorage.deleteByTypeAndId("type", "place:0001_NOT_FOUND");
	}

	@Test
	public void testUpdate() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		DataGroup dataGroup2 = createDataGroupWithRecordInfo();
		dataGroup2.addChild(new DataAtomicSpy("childId2", "childValue2"));
		recordStorage.update("type", "place:0001", dataGroup2, storageTerms, emptyLinkList,
				dataDivider);

		DataGroup dataGroupOut2 = recordStorage.read(List.of("type"), "place:0001");

		dataCopierFactory.MCR.assertNumberOfCallsToMethod("factorForDataElement",
				NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD + 3);
		dataCopierFactory.MCR.assertParameters("factorForDataElement",
				NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD + 1, dataGroup2);
		DataCopierSpy dataCopier = (DataCopierSpy) dataCopierFactory.MCR
				.getReturnValue("factorForDataElement", NO_OF_DATACOPIER_DONE_BY_BEFORE_METHOD + 2);
		dataCopier.MCR.assertReturn("copy", 0, dataGroupOut2);
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
	}

	@Test
	public void testUpdateAndReadLinkList() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		Set<Link> linkList = createLinkListWithTwoLinks();
		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);

		// update
		Set<Link> linkListOne = createLinkListWithOneLink(FROM_RECORD_ID);
		recordStorage.update(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkListOne,
				dataDivider);
	}

	private Set<Link> createLinkListWithOneLink(String fromRecordId) {
		return Set.of(new Link(TO_RECORD_TYPE, TO_RECORD_ID));
	}

	@Test
	public void testUpdateGenerateLinksPointToRecordAreRemovedAndAdded() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		Set<Link> linkList = createLinkListWithTwoLinks();

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
		Set<Link> linkList = createLinkListWithThreeLinksTwoOfThemFromSameRecord(FROM_RECORD_ID);

		recordStorage.create(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 1);
		// update
		linkList = createLinkListWithTwoLinksFromDifferentRecords(FROM_RECORD_ID);
		recordStorage.update(FROM_RECORD_TYPE, FROM_RECORD_ID, dataGroup, storageTerms, linkList,
				dataDivider);
		assertNoOfLinksPointingToRecord(TO_RECORD_TYPE, TO_RECORD_ID, 1);
	}

	private Set<Link> createLinkListWithThreeLinksTwoOfThemFromSameRecord(String fromRecordId) {
		Link link1 = new Link(TO_RECORD_TYPE, TO_RECORD_ID);
		Link link3 = new Link(TO_RECORD_TYPE, "notSame");
		return Set.of(link1, link3);
	}

	private Set<Link> createLinkListWithTwoLinksFromDifferentRecords(String fromRecordId) {
		Set<Link> links = new LinkedHashSet<>();
		links.add(new Link(TO_RECORD_TYPE, TO_RECORD_ID));
		links.add(new Link(TO_RECORD_TYPE, "toRecordId2"));
		return links;
	}

	private void assertNoOfLinksPointingToRecord(String toRecordType, String toRecordId,
			int expectedNoOfLinksPointingToRecord) {
		Collection<Link> generatedLinksPointToRecord = recordStorage.getLinksToRecord(toRecordType,
				toRecordId);
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

		dataCopierFactory.MCR.assertParameters("factorForDataElement", 3, dataGroup);
	}

	@Test
	public void testRecordExistForRecordTypeAndRecordId() {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("type", "place:0001", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		assertTrue(recordStorage.recordExists(List.of("type"), "place:0001"));
	}

	@Test
	public void testRecordNOTExistForRecordTypeAndRecordIdMissingRecordId() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("place", "place:0004", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		assertFalse(recordStorage.recordExists(List.of("place"), "NOTplace:0001"));
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

		assertFalse(recordStorage.recordExists(List.of("NOTtype"), "place:0002"));
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

		assertFalse(recordStorage.recordExists(List.of("abstractRecordType"), "someType:0001"));
	}

	@Test
	public void testRecordExistForNotAbstractNoRecordsExists() {
		DataGroup abstractRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("notAbstractRecordType", "true",
						"false");
		recordStorage.create("recordType", "notAbstractRecordType", abstractRecordType,
				storageTerms, emptyLinkList, dataDivider);

		assertFalse(recordStorage.recordExists(List.of("notAbstractRecordType"), "someType:0001"));
	}

	@Test
	public void testRecordNOTExistForAbstractRecordTypeAndRecordIdMissingRecordId() {
		recordStorage = TestDataRecordInMemoryStorage.createRecordStorageInMemoryWithTestData();

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("image", "image:0004", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		assertFalse(recordStorage.recordExists(List.of("binary"), "NOTimage:0004"));
	}

	@Test
	public void testRecordNOTExistForAbstractRecordTypeAndRecordIdRecordIdNoRecordTypeExist() {
		recordStorage = new RecordStorageInMemory();
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		dataGroup.addChild(new DataAtomicSpy("childId", "childValue"));
		recordStorage.create("image", "image:0004", dataGroup, storageTerms, emptyLinkList,
				dataDivider);

		assertFalse(recordStorage.recordExists(List.of("binary"), "NOTimage:0004"));
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
