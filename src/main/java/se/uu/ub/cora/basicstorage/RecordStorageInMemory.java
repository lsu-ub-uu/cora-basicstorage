/*
 * Copyright 2015, 2017, 2018, 2020, 2021, 2023 Uppsala University Library
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.DataProvider;
import se.uu.ub.cora.data.DataRecordGroup;
import se.uu.ub.cora.data.collected.Link;
import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.data.copier.DataCopier;
import se.uu.ub.cora.data.copier.DataCopierProvider;
import se.uu.ub.cora.storage.Filter;
import se.uu.ub.cora.storage.RecordConflictException;
import se.uu.ub.cora.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.storage.StorageReadResult;

public class RecordStorageInMemory implements RecordStorage {
	private static final String RECORD_TYPE = "recordType";
	private static final String NO_RECORDS_EXISTS_MESSAGE = "No records exists with recordType: ";
	private static final String NO_RECORD_EXISTS_MESSAGE = "No record exists with recordType: ";

	protected Map<String, Map<String, DividerGroup>> records = new HashMap<>();
	protected CollectedTermsHolder collectedTermsHolder = new CollectedTermsInMemoryStorage();

	protected Map<Link, Set<Link>> outgoingLinks = new HashMap<>();
	protected Map<Link, Set<Link>> incommingLinks = new HashMap<>();

	public RecordStorageInMemory() {
		// Make it possible to use default empty record storage
	}

	RecordStorageInMemory(Map<String, Map<String, DividerGroup>> records) {
		throwErrorIfConstructorArgumentIsNull(records);
		this.records = records;
	}

	private final void throwErrorIfConstructorArgumentIsNull(
			Map<String, Map<String, DividerGroup>> records) {
		if (null == records) {
			throw new IllegalArgumentException("Records must not be null");
		}
	}

	@Override
	public void create(String recordType, String recordId, DataGroup record,
			Set<StorageTerm> storageTerms, Set<Link> links, String dataDivider) {
		ensureStorageExistsForRecordType(recordType);
		checkNoConflictOnRecordId(recordType, recordId);
		storeIndependentRecordByRecordTypeAndRecordId(recordType, recordId, record, dataDivider);
		collectedTermsHolder.storeCollectedTerms(recordType, recordId, storageTerms, dataDivider);
		storeLinks(recordType, recordId, links);
	}

	protected final void ensureStorageExistsForRecordType(String recordType) {
		if (holderForRecordTypeDoesNotExistInStorage(recordType)) {
			createHolderForRecordTypeInStorage(recordType);
		}
	}

	private final boolean holderForRecordTypeDoesNotExistInStorage(String recordType) {
		return !records.containsKey(recordType);
	}

	private final void createHolderForRecordTypeInStorage(String recordType) {
		records.put(recordType, new HashMap<>());
	}

	private void checkNoConflictOnRecordId(String recordType, String recordId) {
		if (recordIdExistsForRecordType(recordType, recordId)) {
			throw RecordConflictException
					.withMessage("Record with recordId: " + recordId + " already exists");
		}
	}

	private void storeIndependentRecordByRecordTypeAndRecordId(String recordType, String recordId,
			DataGroup record, String dataDivider) {
		DataGroup recordIndependentOfEnteredRecord = createIndependentCopy(record);
		storeRecordByRecordTypeAndRecordId(recordType, recordId, recordIndependentOfEnteredRecord,
				dataDivider);
	}

	private DataGroup createIndependentCopy(DataGroup record) {
		DataCopier dataCopier = DataCopierProvider.getDataCopierUsingDataElement(record);
		return (DataGroup) dataCopier.copy();
	}

	protected void storeRecordByRecordTypeAndRecordId(String recordType, String recordId,
			DataGroup recordIndependentOfEnteredRecord, String dataDivider) {
		records.get(recordType).put(recordId, DividerGroup.withDataDividerAndDataGroup(dataDivider,
				recordIndependentOfEnteredRecord));
	}

	private void storeLinks(String recordType, String recordId, Set<Link> toLinks) {
		Link from = new Link(recordType, recordId);
		if (!toLinks.isEmpty()) {
			outgoingLinks.put(from, toLinks);
			storeLinksInIncomingLinks(from, toLinks);
		}
	}

	private void storeLinksInIncomingLinks(Link from, Set<Link> toLinks) {
		for (Link toLink : toLinks) {
			incommingLinks.computeIfAbsent(toLink, k -> new LinkedHashSet<Link>()).add(from);
		}
	}

	@Override
	public StorageReadResult readList(List<String> types, Filter filter) {
		List<DataGroup> aggregatedRecordList = new ArrayList<>();
		addRecordsToAggregatedRecordList(aggregatedRecordList, types, filter);
		StorageReadResult readResult = new StorageReadResult();
		readResult.listOfDataGroups = aggregatedRecordList;
		readResult.totalNumberOfMatches = aggregatedRecordList.size();
		return readResult;
	}

	private void addRecordsToAggregatedRecordList(List<DataGroup> aggregatedRecordList,
			List<String> implementingChildRecordTypes, Filter filter) {
		for (String implementingRecordType : implementingChildRecordTypes) {
			try {
				readRecordsForTypeAndFilterAndAddToList(implementingRecordType, filter,
						aggregatedRecordList);
			} catch (RecordNotFoundException e) {
				// Do nothing, another implementing child might have records
			}
		}
	}

	private void readRecordsForTypeAndFilterAndAddToList(String implementingRecordType,
			Filter filter, List<DataGroup> aggregatedRecordList) {
		Collection<DataGroup> readList = readListImplementing(implementingRecordType,
				filter).listOfDataGroups;
		aggregatedRecordList.addAll(readList);
	}

	public StorageReadResult readListImplementing(String type, Filter filter) {
		Map<String, DividerGroup> typeDividerRecords = records.get(type);
		throwErrorIfNoRecordOfType(type, typeDividerRecords);

		return getStorageReadResult(type, filter, typeDividerRecords);
	}

	private StorageReadResult getStorageReadResult(String type, Filter filter,
			Map<String, DividerGroup> typeDividerRecords) {
		StorageReadResult readResult = new StorageReadResult();
		Collection<DataGroup> readFromList = readFromList(type, filter, typeDividerRecords);

		List<DataGroup> subList = getSubList(filter, readFromList);

		readResult.listOfDataGroups = subList;
		readResult.totalNumberOfMatches = readFromList.size();
		return readResult;
	}

	private Collection<DataGroup> readFromList(String type, Filter filter,
			Map<String, DividerGroup> typeDividerRecords) {
		if (!filter.include.isEmpty()) {
			return readListWithFilter(type, filter);
		}
		return readListWithoutFilter(typeDividerRecords);
	}

	private Collection<DataGroup> readListWithoutFilter(
			Map<String, DividerGroup> typeDividerRecords) {
		Map<String, DataGroup> typeRecords = addDataGroupToRecordTypeList(typeDividerRecords);
		return typeRecords.values();
	}

	private Collection<DataGroup> readListWithFilter(String type, Filter filter) {
		List<String> foundRecordIdsForFilter = collectedTermsHolder.findRecordIdsForFilter(type,
				filter);
		return readRecordsForTypeAndListOfIds(type, foundRecordIdsForFilter);
	}

	private Collection<DataGroup> readRecordsForTypeAndListOfIds(String type,
			List<String> foundRecordIdsForFilter) {
		List<DataGroup> foundRecords = new ArrayList<>(foundRecordIdsForFilter.size());
		for (String foundRecordId : foundRecordIdsForFilter) {
			foundRecords.add(read(List.of(type), foundRecordId));
		}
		return foundRecords;
	}

	private List<DataGroup> getSubList(Filter filter, Collection<DataGroup> readFromList) {
		ArrayList<DataGroup> arrayList = new ArrayList<>(readFromList);
		int calculateFromNum = calculateFromNum(filter);
		int calculateToNum = calculateToNum(filter, arrayList.size());
		return arrayList.subList(calculateFromNum, calculateToNum);
	}

	private int calculateFromNum(Filter filter) {
		if (!filter.fromNoIsDefault()) {
			return Math.toIntExact(filter.fromNo - 1);
		}
		return 0;
	}

	private int calculateToNum(Filter filter, int listSize) {
		if (!filter.toNoIsDefault()) {
			return getToNumFromFilterOrListSize(filter, listSize);
		}
		return listSize;
	}

	private int getToNumFromFilterOrListSize(Filter filter, int listSize) {
		long atomicValueAsInteger = filter.toNo;
		return (atomicValueAsInteger < listSize) ? Math.toIntExact(atomicValueAsInteger) : listSize;
	}

	private void throwErrorIfNoRecordOfType(String type,
			Map<String, DividerGroup> typeDividerRecords) {
		if (null == typeDividerRecords) {
			throw RecordNotFoundException.withMessage(NO_RECORDS_EXISTS_MESSAGE + type);
		}
	}

	private Map<String, DataGroup> addDataGroupToRecordTypeList(
			Map<String, DividerGroup> typeDividerRecords) {
		Map<String, DataGroup> typeRecords = new HashMap<>(typeDividerRecords.size());
		for (Entry<String, DividerGroup> entry : typeDividerRecords.entrySet()) {
			DataGroup copyOfRecord = createIndependentCopy(entry.getValue().dataGroup);
			typeRecords.put(entry.getKey(), copyOfRecord);
		}
		return typeRecords;
	}

	// public StorageReadResult readAbstractList(String type, Filter filter) {
	// List<DataGroup> aggregatedRecordList = new ArrayList<>();
	// List<String> implementingChildRecordTypes = findImplementingChildRecordTypes(type);
	//
	// addRecordsToAggregatedRecordList(aggregatedRecordList, implementingChildRecordTypes,
	// filter);
	// addRecordsForParentIfParentIsNotAbstract(type, filter, aggregatedRecordList);
	// throwErrorIfEmptyAggregatedList(type, aggregatedRecordList);
	// StorageReadResult readResult = new StorageReadResult();
	// readResult.listOfDataGroups = aggregatedRecordList;
	// readResult.totalNumberOfMatches = aggregatedRecordList.size();
	// return readResult;
	// }

	// private List<String> findImplementingChildRecordTypes(String type) {
	// Map<String, DividerGroup> allRecordTypes = records.get(RECORD_TYPE);
	// List<String> implementingRecordTypes = new ArrayList<>();
	// return findImplementingChildRecordTypesUsingTypeAndRecordTypeList(type, allRecordTypes,
	// implementingRecordTypes);
	// }

	private List<String> findImplementingChildRecordTypesUsingTypeAndRecordTypeList(String type,
			Map<String, DividerGroup> allRecordTypes, List<String> implementingRecordTypes) {
		for (Entry<String, DividerGroup> entry : allRecordTypes.entrySet()) {
			checkIfChildAndAddToList(type, implementingRecordTypes, entry);
		}
		return implementingRecordTypes;
	}

	private void checkIfChildAndAddToList(String type, List<String> implementingRecordTypes,
			Entry<String, DividerGroup> entry) {
		DataGroup dataGroup = extractDataGroupFromDataDividerGroup(entry);
		String recordTypeId = entry.getKey();

		if (isImplementingChild(type, dataGroup)) {
			implementingRecordTypes.add(recordTypeId);
			findImplementingChildRecordTypesUsingTypeAndRecordTypeList(entry.getKey(),
					records.get(RECORD_TYPE), implementingRecordTypes);
		}
	}

	private DataGroup extractDataGroupFromDataDividerGroup(Entry<String, DividerGroup> entry) {
		DividerGroup dividerGroup = entry.getValue();
		return dividerGroup.dataGroup;
	}

	private boolean isImplementingChild(String type, DataGroup dataGroup) {
		if (dataGroup.containsChildWithNameInData("parentId")) {
			String parentId = extractParentId(dataGroup);
			if (parentId.equals(type)) {
				return true;
			}
		}
		return false;
	}

	private String extractParentId(DataGroup dataGroup) {
		DataGroup parent = dataGroup.getFirstGroupWithNameInData("parentId");
		return parent.getFirstAtomicValueWithNameInData("linkedRecordId");
	}

	// private boolean parentRecordTypeIsNotAbstract(DataGroup recordTypeDataGroup) {
	// return !recordTypeIsAbstract(recordTypeDataGroup);
	// }

	// private void addRecordsForParentIfParentIsNotAbstract(String type, Filter filter,
	// List<DataGroup> aggregatedRecordList) {
	// DataGroup recordTypeDataGroup = returnRecordIfExisting(RECORD_TYPE, type);
	// if (parentRecordTypeIsNotAbstract(recordTypeDataGroup)) {
	// readRecordsForTypeAndFilterAndAddToList(type, filter, aggregatedRecordList);
	// }
	// }
	//
	// private void throwErrorIfEmptyAggregatedList(String type,
	// List<DataGroup> aggregatedRecordList) {
	// if (aggregatedRecordList.isEmpty()) {
	// throw RecordNotFoundException.withMessage(NO_RECORDS_EXISTS_MESSAGE + type);
	// }
	// }

	@Override
	public boolean recordExists(List<String> recordTypes, String recordId) {

		for (String childType : recordTypes) {
			if (recordsExistForRecordType(childType)
					&& recordIdExistsForRecordType(childType, recordId)) {
				return true;
			}
		}
		return false;
	}

	public boolean recordsExistForRecordType(String type) {
		return records.get(type) != null;
	}

	private boolean recordIdExistsForRecordType(String recordType, String recordId) {
		return records.get(recordType).containsKey(recordId);
	}

	// private boolean recordTypeIsAbstract(DataGroup recordTypeDataGroup) {
	// String abstractValue = recordTypeDataGroup.getFirstAtomicValueWithNameInData("abstract");
	// return valueIsAbstract(abstractValue);
	// }

	// private boolean valueIsAbstract(String typeIsAbstract) {
	// return "true".equals(typeIsAbstract);
	// }

	@Override
	public DataRecordGroup read(String type, String id) {
		DataGroup dataGroup = returnRecordIfExisting(type, id);
		DataGroup independentCopy = createIndependentCopy(dataGroup);
		return DataProvider.createRecordGroupFromDataGroup(independentCopy);
	}

	@Override
	public DataGroup read(List<String> types, String recordId) {
		return createIndependentCopy(readRecordFromImplementingRecordTypes(types, recordId));
	}

	private DataGroup readRecordFromImplementingRecordTypes(List<String> types, String recordId) {
		DataGroup readRecord = tryToReadRecordFromImplementingRecordTypes(types, recordId);
		if (readRecord == null) {
			throw RecordNotFoundException
					.withMessage("No record exists with recordId: " + recordId);
		}
		return readRecord;
	}

	private DataGroup tryToReadRecordFromImplementingRecordTypes(List<String> types,
			String recordId) {
		DataGroup readRecord = null;
		for (String implementingType : types) {
			try {
				readRecord = returnRecordIfExisting(implementingType, recordId);
			} catch (RecordNotFoundException e) {
				// Do nothing, another implementing child might have records
			}
		}
		return readRecord;
	}

	private DataGroup returnRecordIfExisting(String recordType, String recordId) {
		checkRecordExists(recordType, recordId);
		return records.get(recordType).get(recordId).dataGroup;
	}

	private void checkRecordExists(String recordType, String recordId) {
		if (holderForRecordTypeDoesNotExistInStorage(recordType)) {
			throw RecordNotFoundException.withMessage(NO_RECORD_EXISTS_MESSAGE + recordType);
		}
		if (null == records.get(recordType).get(recordId)) {
			throw RecordNotFoundException
					.withMessage("No record exists with recordId: " + recordId);
		}
	}

	@Override
	public void deleteByTypeAndId(String recordType, String recordId) {
		checkRecordExists(recordType, recordId);
		removeLinks(recordType, recordId);

		collectedTermsHolder.removePreviousCollectedStorageTerms(recordType, recordId);
		records.get(recordType).remove(recordId);
		if (records.get(recordType).isEmpty()) {
			records.remove(recordType);
		}
	}

	private void removeLinks(String recordType, String recordId) {
		Link from = new Link(recordType, recordId);
		if (outgoingLinks.containsKey(from)) {
			removeIncommingLinks(from);
			removeOutgoingLinks(from);
		}
	}

	private void removeOutgoingLinks(Link from) {
		outgoingLinks.remove(from);
	}

	private void removeIncommingLinks(Link from) {
		for (Link to : outgoingLinks.get(from)) {
			incommingLinks.get(to).remove(from);
			if (incommingLinks.get(to).isEmpty()) {
				incommingLinks.remove(to);
			}
		}
	}

	@Override
	public Set<Link> getLinksToRecord(String type, String id) {
		Link to = new Link(type, id);
		if (incommingLinks.containsKey(to)) {
			return incommingLinks.get(to);
		}
		return Collections.emptySet();
	}

	@Override
	public boolean linksExistForRecord(String type, String id) {
		Link to = new Link(type, id);
		return incommingLinks.containsKey(to);
	}

	@Override
	public void update(String recordType, String recordId, DataGroup record,
			Set<StorageTerm> storageTerms, Set<Link> links, String dataDivider) {
		checkRecordExists(recordType, recordId);
		removeLinks(recordType, recordId);
		storeIndependentRecordByRecordTypeAndRecordId(recordType, recordId, record, dataDivider);
		ensureStorageExistsForRecordType(recordType);
		collectedTermsHolder.storeCollectedTerms(recordType, recordId, storageTerms, dataDivider);
		storeLinks(recordType, recordId, links);
	}

	@Override
	public long getTotalNumberOfRecordsForTypes(List<String> types, Filter filter) {
		long size = 0;
		for (String type : types) {
			size += getTotalNumberOfRecordsForImplementingType(type, filter);
		}
		return getTotalNumberUsingLimitInFilter(size, filter);

	}

	private long getNumberOfRecords(String type, Filter filter) {
		if (!filter.include.isEmpty()) {
			return collectedTermsHolder.findRecordIdsForFilter(type, filter).size();
		}
		return records.get(type).size();
	}

	private long getTotalNumberUsingLimitInFilter(long numberOfRecords, Filter filter) {
		long toNo = getToNoOrNumOfMatchingRecords(filter, numberOfRecords);
		return !filter.fromNoIsDefault() ? getTotalNumberUsingFrom(filter, toNo) : toNo;
	}

	private long getToNoOrNumOfMatchingRecords(Filter filter, long numOfRecordsMatchingFilter) {
		if (filter.toNoIsDefault()) {
			return numOfRecordsMatchingFilter;
		}
		Long toNo = filter.toNo;
		return toNo > numOfRecordsMatchingFilter ? numOfRecordsMatchingFilter : toNo;
	}

	private long getTotalNumberUsingFrom(Filter filter, Long toNo) {
		Long fromNo = filter.fromNo;
		return fromNo > toNo ? 0 : toNo - fromNo + 1;
	}

	void setCollectedTermsHolder(CollectedTermsHolder termsHolder) {
		collectedTermsHolder = termsHolder;

	}

	private long getTotalNumberOfRecordsForImplementingType(String type, Filter filter) {
		if (recordsExistForRecordType(type)) {
			return getNumberOfRecords(type, filter);
		}
		return 0;
	}

}
