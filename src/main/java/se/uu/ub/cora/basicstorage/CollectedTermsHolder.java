/*
 * Copyright 2021 Uppsala University Library
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
import java.util.Map;

import se.uu.ub.cora.data.DataGroup;

public interface CollectedTermsHolder {

	void storeCollectedTerms(String recordType, String recordId, DataGroup collectedTerms,
			String dataDivider);

	List<String> findRecordIdsForFilter(String type, DataGroup filter);

	void removePreviousCollectedStorageTerms(String recordType, String recordId);

	void storeCollectedStorageTermData(String recordType, String storageKey, String recordId,
			StorageTermData storageTermData);

	Map<String, DataGroup> structureCollectedTermsForDisk();

}
