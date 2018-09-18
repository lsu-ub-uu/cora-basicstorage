/*
 * Copyright 2016 Olov McKie
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
package se.uu.ub.cora.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import se.uu.ub.cora.bookkeeper.data.DataElement;
import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.bookkeeper.data.DataPart;
import se.uu.ub.cora.bookkeeper.data.converter.DataGroupToJsonConverter;
import se.uu.ub.cora.bookkeeper.data.converter.JsonToDataConverter;
import se.uu.ub.cora.bookkeeper.data.converter.JsonToDataConverterFactory;
import se.uu.ub.cora.bookkeeper.data.converter.JsonToDataConverterFactoryImp;
import se.uu.ub.cora.bookkeeper.storage.MetadataStorage;
import se.uu.ub.cora.json.builder.org.OrgJsonBuilderFactoryAdapter;
import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.JsonValue;
import se.uu.ub.cora.json.parser.org.OrgJsonParser;
import se.uu.ub.cora.spider.record.storage.RecordStorage;

public class RecordStorageOnDisk extends RecordStorageInMemory
		implements RecordStorage, MetadataStorage {
	private static final String COLLECTED_DATA = "collectedData";
	private static final String LINK_LISTS = "linkLists";
	private static final String JSON_FILE_END = ".json";
	private Set<String> allSeenCollectedDataFileNames = new HashSet<>();
	private String basePath;

	protected RecordStorageOnDisk(String basePath) {
		this.basePath = basePath;
		tryToReadStoredDataFromDisk();
	}

	public static RecordStorageOnDisk createRecordStorageOnDiskWithBasePath(String basePath) {
		return new RecordStorageOnDisk(basePath);
	}

	private void tryToReadStoredDataFromDisk() {
		Stream<Path> list = Stream.empty();
		try {
			list = Files.list(Paths.get(basePath));

			readStoredDataFromDisk(list);
		} catch (IOException e) {
			throw DataStorageException.withMessage("can not read files from disk on init" + e);
		} finally {
			list.close();
		}
	}

	private void readStoredDataFromDisk(Stream<Path> list) throws IOException {
		Iterator<Path> iterator = list.iterator();
		while (iterator.hasNext()) {
			readFileOrReadDirectoryIfNotStreamsDir(iterator);
		}
	}

	private void readFileOrReadDirectoryIfNotStreamsDir(Iterator<Path> iterator)
			throws IOException {
		Path path = iterator.next();
		if (Files.isDirectory(path)) {
			if (!path.endsWith("streams/")) {
				Stream<Path> list = Files.list(path);
				readStoredDataFromDisk(list);

			}
		} else {
			readFileAndParseFileByPath(path);
		}
	}

	private void readFileAndParseFileByPath(Path path) throws IOException {
		String fileNameTypePart = getTypeFromPath(path);
		String dataDivider = getDataDividerFromPath(path);
		List<DataElement> recordsFromFile = extractChildrenFromFileByPath(path);

		if (fileContainsLinkLists(fileNameTypePart)) {
			parseAndStoreDataLinksInMemory(dataDivider, recordsFromFile);
		} else if (fileNameTypePart.equals(COLLECTED_DATA)) {
			allSeenCollectedDataFileNames.add(dataDivider);
			parseAndStoreCollectedStorageTermsInMemory(recordsFromFile);
		} else {
			parseAndStoreRecordsInMemory(fileNameTypePart, dataDivider, recordsFromFile);
		}
	}

	private List<DataElement> extractChildrenFromFileByPath(Path path) throws IOException {
		String json = readJsonFileByPath(path);
		DataGroup recordList = convertJsonStringToDataGroup(json);
		return recordList.getChildren();
	}

	private String readJsonFileByPath(Path path) throws IOException {
		StringBuilder jsonBuilder = new StringBuilder();
		BufferedReader reader = null;
		try {
			reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
			String line;
			while ((line = reader.readLine()) != null) {
				jsonBuilder.append(line);
			}
		} finally {
			reader.close();
		}
		return jsonBuilder.toString();
	}

	private DataGroup convertJsonStringToDataGroup(String jsonRecord) {
		JsonParser jsonParser = new OrgJsonParser();
		JsonValue jsonValue = jsonParser.parseString(jsonRecord);
		JsonToDataConverterFactory jsonToDataConverterFactory = new JsonToDataConverterFactoryImp();
		JsonToDataConverter jsonToDataConverter = jsonToDataConverterFactory
				.createForJsonObject(jsonValue);
		DataPart dataPart = jsonToDataConverter.toInstance();
		return (DataGroup) dataPart;
	}

	private boolean fileContainsLinkLists(String fileNameTypePart) {
		return fileNameTypePart.equals(LINK_LISTS);
	}

	private void parseAndStoreDataLinksInMemory(String dataDivider, List<DataElement> recordTypes) {
		for (DataElement typesElement : recordTypes) {
			parseAndStoreRecordTypeDataLinksInMemory(dataDivider, typesElement);
		}
	}

	private void parseAndStoreRecordTypeDataLinksInMemory(String dataDivider,
			DataElement typesElement) {
		DataGroup recordType = (DataGroup) typesElement;
		String recordTypeName = recordType.getNameInData();
		ensureStorageExistsForRecordType(recordTypeName);

		List<DataElement> records = recordType.getChildren();
		for (DataElement recordElement : records) {
			parseAndStoreRecordDataLinksInMemory(dataDivider, recordTypeName, recordElement);
		}
	}

	private void parseAndStoreRecordDataLinksInMemory(String dataDivider, String recordTypeName,
			DataElement recordElement) {
		DataGroup record = (DataGroup) recordElement;
		String recordId = record.getNameInData();
		DataGroup collectedDataLinks = (DataGroup) record
				.getFirstChildWithNameInData("collectedDataLinks");
		storeLinks(recordTypeName, recordId, collectedDataLinks, dataDivider);
	}

	private void parseAndStoreCollectedStorageTermsInMemory(List<DataElement> recordsFromFile) {
		for (DataElement storageTerm : recordsFromFile) {
			parseAndStoreCollectedStorageTermInMemory((DataGroup) storageTerm);
		}
	}

	private void parseAndStoreCollectedStorageTermInMemory(DataGroup storageTerm) {
		String type = storageTerm.getFirstAtomicValueWithNameInData("type");
		String key = storageTerm.getFirstAtomicValueWithNameInData("key");
		String id = storageTerm.getFirstAtomicValueWithNameInData("id");
		StorageTermData storageTermData = createStorageTermData(storageTerm);
		collectedTermsHolder.storeCollectedStorageTermData(type, key, id, storageTermData);
	}

	private StorageTermData createStorageTermData(DataGroup storageTerm) {
		String value = storageTerm.getFirstAtomicValueWithNameInData("value");
		String dataDivider = storageTerm.getFirstAtomicValueWithNameInData("dataDivider");
		return StorageTermData.withValueAndDataDivider(value, dataDivider);
	}

	private void parseAndStoreRecordsInMemory(String fileNameTypePart, String dataDivider,
			List<DataElement> recordTypes) {
		ensureStorageExistsForRecordType(fileNameTypePart);

		for (DataElement dataElement : recordTypes) {
			parseAndStoreRecordInMemory(fileNameTypePart, dataDivider, dataElement);
		}
	}

	private void parseAndStoreRecordInMemory(String fileNameTypePart, String dataDivider,
			DataElement dataElement) {
		DataGroup record = (DataGroup) dataElement;

		DataGroup recordInfo = record.getFirstGroupWithNameInData("recordInfo");
		String recordId = recordInfo.getFirstAtomicValueWithNameInData("id");

		storeRecordByRecordTypeAndRecordId(fileNameTypePart, recordId, record, dataDivider);
	}

	private String getDataDividerFromPath(Path path) {
		String fileName2 = path.getFileName().toString();
		return fileName2.substring(fileName2.lastIndexOf('_') + 1, fileName2.indexOf('.'));
	}

	private String getTypeFromPath(Path path) {
		String fileName = path.getFileName().toString();
		return fileName.substring(0, fileName.lastIndexOf('_'));
	}

	@Override
	public synchronized void create(String recordType, String recordId, DataGroup record,
			DataGroup collectedTerms, DataGroup linkList, String dataDivider) {
		super.create(recordType, recordId, record, collectedTerms, linkList, dataDivider);
		writeDataToDisk(recordType, dataDivider);
	}

	protected void writeDataToDisk(String recordType, String dataDivider) {
		writeRecordsToDisk(recordType, dataDivider);
		writeCollectedDataToDisk();
		writeLinkListToDisk(dataDivider);
	}

	private void writeCollectedDataToDisk() {
		Map<String, DataGroup> collectedDataByDataDivider = collectedTermsHolder
				.structureCollectedTermsForDisk();
		removePreviousCollectedDataFiles();
		if (!collectedDataByDataDivider.isEmpty()) {
			writeCollectedDataForDataDividersToDisk(collectedDataByDataDivider);
		}
	}

	private void removePreviousCollectedDataFiles() {
		for (String dataDivider : allSeenCollectedDataFileNames) {
			String collectedDataFileName = COLLECTED_DATA + "_" + dataDivider + JSON_FILE_END;
			Path path = Paths.get(basePath, dataDivider, collectedDataFileName);
			if (path.toFile().exists()) {
				removeFileFromDisk(COLLECTED_DATA, dataDivider);
			}
		}
	}

	private void writeCollectedDataForDataDividersToDisk(
			Map<String, DataGroup> collectedDataByDataDivider) {
		for (Entry<String, DataGroup> entry : collectedDataByDataDivider.entrySet()) {
			String dataDivider = entry.getKey();
			Path path = Paths.get(basePath, dataDivider,
					"collectedData_" + dataDivider + JSON_FILE_END);
			tryToWriteDataGroupToDiskAsJson(path, entry.getValue());
			allSeenCollectedDataFileNames.add(dataDivider);
		}
	}

	private void writeRecordsToDisk(String recordType, String dataDivider) {
		if (recordsExistForRecordType(recordType)) {
			writeRecordsToDiskWhereRecordTypeExists(recordType, dataDivider);
		} else {
			removeFileFromDisk(recordType, dataDivider);
		}
	}

	private void removeFileFromDisk(String recordType, String dataDivider) {
		String recordTypeFileName = recordType + "_" + dataDivider + JSON_FILE_END;
		try {
			Path path = Paths.get(basePath, dataDivider, recordTypeFileName);
			Files.delete(path);
			deleteDirectoryIfEmpty(dataDivider);
		} catch (IOException e) {
			throw DataStorageException.withMessage("can not delete record files from disk" + e);
		}
	}

	private void deleteDirectoryIfEmpty(String dataDivider) {
		File directoryIncludingDataDivider = Paths.get(basePath, dataDivider).toFile();
		String[] list = directoryIncludingDataDivider.list();
		if (list.length == 0) {
			directoryIncludingDataDivider.delete();
		}
	}

	private void writeRecordsToDiskWhereRecordTypeExists(String recordType, String dataDivider) {
		Map<String, DataGroup> recordLists = divideRecordTypeDataByDataDivider(recordType);
		writeDividedRecordsToDisk(recordType, recordLists);
		possiblyRemoveOldDataDividerFile(recordType, dataDivider, recordLists);
	}

	private Map<String, DataGroup> divideRecordTypeDataByDataDivider(String recordType) {
		Map<String, DividerGroup> readList = records.get(recordType);
		Map<String, DataGroup> recordLists = new HashMap<>();
		for (Entry<String, DividerGroup> dividerEntry : readList.entrySet()) {
			addRecordToListBasedOnDataDivider(recordLists, dividerEntry);
		}
		return recordLists;
	}

	private void addRecordToListBasedOnDataDivider(Map<String, DataGroup> recordLists,
			Entry<String, DividerGroup> dividerEntry) {
		DividerGroup dividerGroup = dividerEntry.getValue();
		String currentDataDivider = dividerGroup.dataDivider;
		DataGroup currentDataGroup = dividerGroup.dataGroup;
		ensureListForDataDivider(recordLists, currentDataDivider);
		recordLists.get(currentDataDivider).addChild(currentDataGroup);
	}

	private void ensureListForDataDivider(Map<String, DataGroup> recordLists,
			String currentDataDivider) {
		if (!recordLists.containsKey(currentDataDivider)) {
			recordLists.put(currentDataDivider, DataGroup.withNameInData("recordList"));
		}
	}

	private void writeDividedRecordsToDisk(String recordType, Map<String, DataGroup> recordLists) {
		for (Entry<String, DataGroup> recordListEntry : recordLists.entrySet()) {
			String dataDivider = recordListEntry.getKey();
			DataGroup dataGroupRecord = recordListEntry.getValue();

			possiblyCreateFolderForDataDivider(dataDivider);
			Path path = Paths.get(basePath, dataDivider,
					recordType + "_" + dataDivider + JSON_FILE_END);
			tryToWriteDataGroupToDiskAsJson(path, dataGroupRecord);
		}
	}

	private void possiblyCreateFolderForDataDivider(String dataDivider) {
		Path pathIncludingDataDivider = Paths.get(basePath, dataDivider);
		File newPath = pathIncludingDataDivider.toFile();
		if (!newPath.exists()) {
			newPath.mkdir();
		}
	}

	private void tryToWriteDataGroupToDiskAsJson(Path path, DataGroup dataGroup) {
		String json = convertDataGroupToJsonString(dataGroup);
		try {
			writeDataGroupToDiskAsJson(path, json);
		} catch (IOException | NullPointerException e) {
			throw DataStorageException.withMessage("can not write files to disk" + e);
		}
	}

	private void writeDataGroupToDiskAsJson(Path path, String json) throws IOException {
		BufferedWriter writer = null;
		try {
			if (Files.exists(path)) {
				Files.delete(path);
			}
			writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
					StandardOpenOption.CREATE);
			writer.write(json, 0, json.length());
			writer.flush();
		} finally {
			writer.close();
		}
	}

	private String convertDataGroupToJsonString(DataGroup dataGroup) {
		DataGroupToJsonConverter dataToJsonConverter = createDataGroupToJsonConvert(dataGroup);
		return dataToJsonConverter.toJson();
	}

	private DataGroupToJsonConverter createDataGroupToJsonConvert(DataGroup dataGroup) {
		se.uu.ub.cora.json.builder.JsonBuilderFactory jsonBuilderFactory = new OrgJsonBuilderFactoryAdapter();
		return DataGroupToJsonConverter.usingJsonFactoryForDataGroup(jsonBuilderFactory, dataGroup);
	}

	private void possiblyRemoveOldDataDividerFile(String recordType, String dataDivider,
			Map<String, DataGroup> recordLists) {
		if (!recordLists.containsKey(dataDivider)) {
			removeFileFromDisk(recordType, dataDivider);
		}
	}

	private void writeLinkListToDisk(String dataDivider) {
		Map<String, DataGroup> linkListsGroups = new HashMap<>();
		divideLinkListsByDataDivider(linkListsGroups);
		writeDividedLinkListsToDisk(linkListsGroups);
		possiblyRemoveOldDataDividerLinkListFile(dataDivider, linkListsGroups);
	}

	private void divideLinkListsByDataDivider(Map<String, DataGroup> linkListsGroups) {
		for (Entry<String, Map<String, DividerGroup>> recordType : linkLists.entrySet()) {
			addLinkListsForRecordTypeBasedOnDataDivider(linkListsGroups, recordType);
		}
	}

	private void addLinkListsForRecordTypeBasedOnDataDivider(Map<String, DataGroup> linkListsGroups,
			Entry<String, Map<String, DividerGroup>> recordType) {
		Map<String, DividerGroup> recordGroupMap = recordType.getValue();
		for (Entry<String, DividerGroup> recordEntry : recordGroupMap.entrySet()) {
			addLinkListsForRecordToListBasedOnDataDivider(linkListsGroups, recordType, recordEntry);
		}
	}

	private void addLinkListsForRecordToListBasedOnDataDivider(
			Map<String, DataGroup> linkListsGroups,
			Entry<String, Map<String, DividerGroup>> recordType,
			Entry<String, DividerGroup> recordEntry) {
		DividerGroup dataDividerGroup = recordEntry.getValue();
		String currentDataDivider = dataDividerGroup.dataDivider;
		ensureLinkListForDataDivider(linkListsGroups, currentDataDivider);
		ensureLinkListForRecordType(linkListsGroups, recordType, currentDataDivider);

		DataGroup recordTypeChild = (DataGroup) linkListsGroups.get(currentDataDivider)
				.getFirstChildWithNameInData(recordType.getKey());
		addLinkListsForRecord(recordEntry, dataDividerGroup, recordTypeChild);
	}

	private void ensureLinkListForDataDivider(Map<String, DataGroup> linkListsGroups,
			String currentDataDivider) {
		if (!linkListsGroups.containsKey(currentDataDivider)) {
			linkListsGroups.put(currentDataDivider, DataGroup.withNameInData(LINK_LISTS));
		}
	}

	private void ensureLinkListForRecordType(Map<String, DataGroup> linkListsGroups,
			Entry<String, Map<String, DividerGroup>> recordType, String currentDataDivider) {
		if (!linkListsGroups.get(currentDataDivider)
				.containsChildWithNameInData(recordType.getKey())) {
			DataGroup recordTypeGroup = DataGroup.withNameInData(recordType.getKey());
			linkListsGroups.get(currentDataDivider).addChild(recordTypeGroup);
		}
	}

	private void addLinkListsForRecord(Entry<String, DividerGroup> recordEntry,
			DividerGroup dataDividerGroup, DataGroup recordTypeChild) {
		DataGroup recordIdGroup = DataGroup.withNameInData(recordEntry.getKey());
		recordIdGroup.addChild(dataDividerGroup.dataGroup);

		recordTypeChild.addChild(recordIdGroup);
	}

	private void writeDividedLinkListsToDisk(Map<String, DataGroup> linkListsGroups) {
		for (Entry<String, DataGroup> recordListEntry : linkListsGroups.entrySet()) {
			String dataDivider = recordListEntry.getKey();
			Path path = Paths.get(basePath, dataDivider,
					"linkLists_" + dataDivider + JSON_FILE_END);
			tryToWriteDataGroupToDiskAsJson(path, recordListEntry.getValue());
		}
	}

	private void possiblyRemoveOldDataDividerLinkListFile(String dataDivider,
			Map<String, DataGroup> linkListsGroups) {
		if (!linkListsGroups.containsKey(dataDivider)) {
			String linkListFileName = "linkLists_" + dataDivider + JSON_FILE_END;
			Path path = Paths.get(basePath, dataDivider, linkListFileName);
			if (Files.exists(path)) {
				removeFileFromDisk(LINK_LISTS, dataDivider);
			}
		}
	}

	@Override
	public synchronized void update(String recordType, String recordId, DataGroup record,
			DataGroup collectedTerms, DataGroup linkList, String dataDivider) {
		String previousDataDivider = records.get(recordType).get(recordId).dataDivider;
		super.update(recordType, recordId, record, collectedTerms, linkList, dataDivider);
		writeDataToDisk(recordType, previousDataDivider);
	}

	@Override
	public synchronized void deleteByTypeAndId(String recordType, String recordId) {
		String previousDataDivider = records.get(recordType).get(recordId).dataDivider;
		super.deleteByTypeAndId(recordType, recordId);
		writeDataToDisk(recordType, previousDataDivider);
	}
}
