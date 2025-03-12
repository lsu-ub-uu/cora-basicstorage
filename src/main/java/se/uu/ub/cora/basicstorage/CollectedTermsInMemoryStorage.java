/*
 * Copyright 2017, 2021, 2024 Uppsala University Library
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.storage.Condition;
import se.uu.ub.cora.storage.Filter;
import se.uu.ub.cora.storage.Part;

class CollectedTermsInMemoryStorage implements CollectedTermsHolder {
	private Map<String, Map<String, Map<String, List<StorageTermData>>>> terms = new ConcurrentHashMap<>();
	private Map<TypeAndId, Set<StorageTerm>> originalStoredStorageTerms = new ConcurrentHashMap<>();

	@Override
	public void removePreviousCollectedStorageTerms(String recordType, String recordId) {
		if (termsExistForRecordType(recordType)) {
			Map<String, Map<String, List<StorageTermData>>> termsForRecordType = terms
					.get(recordType);
			removePreviousCollectedStorageTermsForRecordType(recordId, termsForRecordType);
		}
		removeOriginalStoredStorageTerms(recordType, recordId);
	}

	private void removeOriginalStoredStorageTerms(String recordType, String recordId) {
		TypeAndId typeAndId = new TypeAndId(recordType, recordId);
		originalStoredStorageTerms.remove(typeAndId);
	}

	private boolean termsExistForRecordType(String recordType) {
		return terms.containsKey(recordType);
	}

	private void removePreviousCollectedStorageTermsForRecordType(String recordId,
			Map<String, Map<String, List<StorageTermData>>> termsForRecordType) {
		for (Entry<String, Map<String, List<StorageTermData>>> keyEntry : termsForRecordType
				.entrySet()) {
			Map<String, List<StorageTermData>> termsForRecordId = keyEntry.getValue();
			removePreviousCollectedStorageTermsForRecordId(recordId, termsForRecordId);
		}
	}

	private void removePreviousCollectedStorageTermsForRecordId(String recordId,
			Map<String, List<StorageTermData>> termsForRecordId) {
		List<String> idsToRemove = new ArrayList<>();
		findIdsToRemove(recordId, termsForRecordId, idsToRemove);
		removeStorageTermsForIds(termsForRecordId, idsToRemove);
	}

	private void findIdsToRemove(String recordId,
			Map<String, List<StorageTermData>> termsForRecordId, List<String> idsToRemove) {
		for (Entry<String, List<StorageTermData>> recordIdEntry : termsForRecordId.entrySet()) {
			if (termsExistForRecordId(recordId, recordIdEntry)) {
				idsToRemove.add(recordIdEntry.getKey());
			}
		}
	}

	private void removeStorageTermsForIds(Map<String, List<StorageTermData>> termsForRecordId,
			List<String> idsToRemove) {
		for (String key : idsToRemove) {
			termsForRecordId.remove(key);
		}
	}

	private boolean termsExistForRecordId(String recordId,
			Entry<String, List<StorageTermData>> recordIdEntry) {
		return recordIdEntry.getKey().equals(recordId);
	}

	@Override
	public void storeCollectedTerms(String recordType, String recordId,
			Set<StorageTerm> storageTerms, String dataDivider) {
		removePreviousCollectedStorageTerms(recordType, recordId);
		for (StorageTerm storageTerm : storageTerms) {
			storeCollectedStorageTerm(recordType, recordId, dataDivider, storageTerm);
		}
		storeOriginalStorageTerms(recordType, recordId, storageTerms);
	}

	private void storeOriginalStorageTerms(String recordType, String recordId,
			Set<StorageTerm> storageTerms) {
		TypeAndId typeAndId = new TypeAndId(recordType, recordId);
		originalStoredStorageTerms.remove(typeAndId);
		if (!storageTerms.isEmpty()) {
			originalStoredStorageTerms.put(typeAndId, storageTerms);
		}
	}

	private void storeCollectedStorageTerm(String recordType, String recordId, String dataDivider,
			StorageTerm storageTerm) {
		String storageKey = storageTerm.storageKey();
		String termValue = storageTerm.value();

		List<StorageTermData> listOfStorageTermData = ensureStorageListExistsForTermForTypeAndKeyAndId(
				recordType, storageKey, recordId);

		listOfStorageTermData.add(StorageTermData.withValueAndDataDivider(termValue, dataDivider));
	}

	private List<StorageTermData> ensureStorageListExistsForTermForTypeAndKeyAndId(
			String recordType, String storageKey, String recordId) {
		ensureStorageMapExistsForRecordType(recordType);
		Map<String, Map<String, List<StorageTermData>>> storageKeysForType = terms.get(recordType);
		ensureStorageListExistsForTermKey(storageKey, storageKeysForType);
		ensureStorageListExistsForId(storageKey, recordId, storageKeysForType);
		return storageKeysForType.get(storageKey).get(recordId);
	}

	private void ensureStorageMapExistsForRecordType(String recordType) {
		if (storageTermExistsForRecordType(recordType)) {
			terms.put(recordType, new ConcurrentHashMap<>());
		}
	}

	private void ensureStorageListExistsForTermKey(String storageKey,
			Map<String, Map<String, List<StorageTermData>>> storageKeysForType) {
		if (!storageKeysForType.containsKey(storageKey)) {
			ConcurrentHashMap<String, List<StorageTermData>> mapOfIds = new ConcurrentHashMap<>();
			storageKeysForType.put(storageKey, mapOfIds);
		}
	}

	private void ensureStorageListExistsForId(String storageKey, String recordId,
			Map<String, Map<String, List<StorageTermData>>> storageKeysForType) {
		if (!storageKeysForType.get(storageKey).containsKey(recordId)) {
			storageKeysForType.get(storageKey).put(recordId, new ArrayList<>());
		}
	}

	@Override
	public List<String> findRecordIdsForFilter(String type, Filter filter) {
		if (storageTermExistsForRecordType(type)) {
			return Collections.emptyList();
		}
		return getRecordIdsForTypeAndFilter(type, filter);
	}

	private List<String> getRecordIdsForTypeAndFilter(String type, Filter filter) {
		Part firstPart = filter.include.get(0);
		List<Condition> conditionsInPart = firstPart.conditions;

		return getRecordIdsForConditionsInPart(type, conditionsInPart);
	}

	private List<String> getRecordIdsForConditionsInPart(String type,
			List<Condition> conditionsInPart) {
		Set<String> foundRecordIds = new HashSet<>();

		for (Condition condition : conditionsInPart) {
			List<String> foundForThisContidition = findRecordIdsMatchingFilterCondition(type,
					condition);
			if (isFirstCondition(condition, conditionsInPart)) {
				foundRecordIds.addAll(foundForThisContidition);
			} else {
				foundRecordIds.retainAll(foundForThisContidition);
			}
		}
		return convertSetToList(foundRecordIds);
	}

	private ArrayList<String> convertSetToList(Set<String> foundRecordIds) {
		return new ArrayList<>(foundRecordIds);
	}

	private boolean isFirstCondition(Condition condition, List<Condition> conditionsInPart) {
		return condition == conditionsInPart.get(0);
	}

	private boolean storageTermExistsForRecordType(String type) {
		return !terms.containsKey(type);
	}

	private List<String> findRecordIdsMatchingFilterCondition(String type, Condition condition) {
		String key = condition.key();
		String value = condition.value();
		Map<String, Map<String, List<StorageTermData>>> storageTermsForRecordType = terms.get(type);

		Map<String, List<StorageTermData>> mapOfIdsAndStorageTermForTypeAndKey = storageTermsForRecordType
				.get(key);
		if (null != mapOfIdsAndStorageTermForTypeAndKey) {
			return findRecordIdsMatchingValueForKey(value, mapOfIdsAndStorageTermForTypeAndKey);
		}
		return Collections.emptyList();
	}

	private List<String> findRecordIdsMatchingValueForKey(String value,
			Map<String, List<StorageTermData>> mapOfIdsAndStorageTermForTypeAndKey) {
		List<String> foundRecordIdsForKey = new ArrayList<>();

		for (Entry<String, List<StorageTermData>> entry : mapOfIdsAndStorageTermForTypeAndKey
				.entrySet()) {
			findRecordIdsMatchingValueForId(value, foundRecordIdsForKey, entry);
		}

		return foundRecordIdsForKey;
	}

	private void findRecordIdsMatchingValueForId(String value, List<String> foundRecordIdsForKey,
			Entry<String, List<StorageTermData>> entry) {
		String key = entry.getKey();
		for (StorageTermData storageTermData : entry.getValue()) {
			if (storageTermData.value.equals(value)) {
				foundRecordIdsForKey.add(key);
			}
		}
	}

	@Override
	public Set<StorageTerm> getCollectTerms(String recordType, String recordId) {
		TypeAndId typeAndId = new TypeAndId(recordType, recordId);
		return originalStoredStorageTerms.getOrDefault(typeAndId, Collections.emptySet());
	}

	public Map<TypeAndId, Set<StorageTerm>> onlyForTestGetOriginalStoredStorageTerms() {
		return originalStoredStorageTerms;
	}

}

record TypeAndId(String type, String id) {
}