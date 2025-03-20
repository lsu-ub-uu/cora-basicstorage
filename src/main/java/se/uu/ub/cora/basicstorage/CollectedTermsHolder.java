/*
 * Copyright 2021, 2025 Uppsala University Library
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

import java.util.List;
import java.util.Set;

import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.storage.Filter;

public interface CollectedTermsHolder {

	/**
	 * storeCollectedTerms method should store the provided storageTerms for the given recordType
	 * and recordId, with the provided dataDivider.
	 * </p>
	 * The stored storageTerms should be retrievable using the
	 * {@link CollectedTermsHolder#getCollectTerms(String, String)} method.
	 * 
	 * @param recordType
	 *            A String with the recordType for the record where the collected terms was
	 *            collected from.
	 * @param recordId
	 *            A String with the recordId for the record where the collected terms was collected
	 *            from.
	 * @param storageTerms
	 *            A Set of StorageTerms with the data from the specified record
	 * @param dataDivider
	 *            The records dataDivider
	 */
	void storeCollectedTerms(String recordType, String recordId, Set<StorageTerm> storageTerms,
			String dataDivider);

	/**
	 * getCollectTerms method should return a set of storage terms for the given recordType and
	 * recordId, with the same data that previously has been stored using the
	 * {@link CollectedTermsHolder#storeCollectedTerms(String, String, Set, String)} method.
	 * </p>
	 * If no collectedTerms has been stored for the provided type and id, should an empty set be
	 * returned.
	 * 
	 * @param recordType
	 *            A String with the recordType
	 * @param recordId
	 *            A String with the recordId
	 * @return a set of storage terms for the given recordType and recordId
	 */
	Set<StorageTerm> getCollectTerms(String recordType, String recordId);

	/**
	 * removePreviousCollectedStorageTerms method should remove any previously stored storageTerms
	 * for the given recordType and recordId.
	 * </p>
	 * If no storageTerms has been stored for the provided type and id, should nothing happen.
	 * 
	 * @param recordType
	 *            A String with the recordType
	 * @param recordId
	 *            A String with the recordId
	 */
	void removePreviousCollectedStorageTerms(String recordType, String recordId);

	/**
	 * findRecordIdsForFilter method should return a list of recordIds for the provided type and
	 * filter. The recordIds should be found by matching the existing stored storageTerms and
	 * {@link Filter} for the provided recordType.
	 * </p>
	 * If no recordIds are found for the provided type and filter, should an empty list be returned.
	 * 
	 * @param recordType
	 *            A String with the type
	 * @param filter
	 *            A {@link Filter} with the filter
	 * @return a list of recordIds for the provided type and filter
	 */
	List<String> findRecordIdsForFilter(String recordType, Filter filter);
}
