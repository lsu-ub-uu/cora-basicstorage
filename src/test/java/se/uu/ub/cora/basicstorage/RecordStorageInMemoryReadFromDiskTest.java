/*
 * Copyright 2016 Uppsala University Library
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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.basicstorage.testdata.DataCreator;
import se.uu.ub.cora.data.DataAtomicFactory;
import se.uu.ub.cora.data.DataAtomicProvider;
import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.DataGroupFactory;
import se.uu.ub.cora.data.DataGroupProvider;
import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.data.copier.DataCopierFactory;
import se.uu.ub.cora.data.copier.DataCopierProvider;

public class RecordStorageInMemoryReadFromDiskTest {
	private static final String FROM_RECORD_TYPE = "fromRecordType";
	private static final String TO_RECORD_ID = "toRecordId";
	private static final String TO_RECORD_TYPE = "toRecordType";
	private String basePath = "/tmp/recordStorageOnDiskTemp/";
	private DataGroup emptyLinkList = DataCreator.createEmptyLinkList();
	private RecordStorageOnDisk recordStorage;
	private DataGroupFactory dataGroupFactory;
	private DataAtomicFactory dataAtomicFactory;
	private DataCopierFactory dataCopierFactory;
	List<StorageTerm> storageTerms = Collections.emptyList();

	@BeforeMethod
	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(basePath);
		dir.mkdir();
		deleteFiles();
		setUpData();
	}

	private void deleteFiles() throws IOException {
		Stream<Path> list;
		list = Files.list(Paths.get(basePath));
		list.forEach(p -> deleteFile(p));
		list.close();
	}

	private void deleteFile(Path path) {
		try {
			Files.delete(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void setUpData() {
		dataGroupFactory = new DataGroupFactorySpy();
		DataGroupProvider.setDataGroupFactory(dataGroupFactory);
		dataAtomicFactory = new DataAtomicFactorySpy();
		DataAtomicProvider.setDataAtomicFactory(dataAtomicFactory);
		dataCopierFactory = new DataCopierFactorySpy();
		DataCopierProvider.setDataCopierFactory(dataCopierFactory);

		DataGroup emptyLinkList = new DataGroupSpy("collectedDataLinks");
		recordStorage = RecordStorageInMemoryReadFromDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup placeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("place", "true", "false");
		recordStorage.create("recordType", "place", placeRecordType, storageTerms, emptyLinkList,
				"cora");
		DataGroup recordTypeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("recordType", "true", "false");
		recordStorage.create("recordType", "recordType", recordTypeRecordType, storageTerms,
				emptyLinkList, "cora");
	}

	@AfterMethod
	public void removeTempFiles() throws IOException {
		if (Files.exists(Paths.get(basePath))) {
			deleteFiles();
			File dir = new File(basePath);
			dir.delete();
		}
	}

	@Test
	public void testInitNoFilesOnDisk() throws IOException {

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		DataGroup readDataGroup = recordStorage.read("place", "place:0001");

		Map<String, DividerGroup> map = recordStorage.records.get("place");
		DividerGroup dividerGroup = map.get("place:0001");

		DataGroup dataGroupInStorage = dividerGroup.dataGroup;
		assertSame(dataGroupInStorage, readDataGroup);

		Path placePath = Paths.get(basePath, "place.json");
		assertFalse(Files.exists(placePath));

		Path path = Paths.get(basePath, "linkLists.json");
		assertFalse(Files.exists(path));
	}

	private DataGroup createDataGroupWithRecordInfo() {
		return DataCreator.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
				"authority", "place", "place:0001");
	}

	@Test
	public void testRecordWithLinks() throws IOException {
		DataGroup linkListWithTwoLinks = createLinkListWithTwoLinks("place:0001");

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, linkListWithTwoLinks,
				"cora");
		DataGroup readDataGroup = recordStorage.read("place", "place:0001");

		Map<String, DividerGroup> map = recordStorage.records.get("place");
		DividerGroup dividerGroup = map.get("place:0001");

		DataGroup dataGroupInStorage = dividerGroup.dataGroup;
		assertSame(dataGroupInStorage, readDataGroup);

		Path placePath = Paths.get(basePath, "place.json");
		assertFalse(Files.exists(placePath));

		Path path = Paths.get(basePath, "linkLists.json");
		assertFalse(Files.exists(path));

		Path path2 = Paths.get(basePath, "incomingLinks.json");
		assertFalse(Files.exists(path2));
	}

	private DataGroup createLinkListWithTwoLinks(String fromRecordId) {
		DataGroup linkList = DataCreator.createEmptyLinkList();

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, "toRecordId2"));
		return linkList;
	}

	@Test
	public void testUpdate() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageInMemoryReadFromDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

		dataGroup.addChild(new DataAtomicSpy("someNameInData", "someValue"));
		recordStorage.update("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

		Path placePath = Paths.get(basePath, "place.json");
		assertFalse(Files.exists(placePath));

		Path path = Paths.get(basePath, "linkLists.json");
		assertFalse(Files.exists(path));

		Path path2 = Paths.get(basePath, "incomingLinks.json");
		assertFalse(Files.exists(path2));
	}

}
