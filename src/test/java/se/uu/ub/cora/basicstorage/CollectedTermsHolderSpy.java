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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.collected.StorageTerm;

public class CollectedTermsHolderSpy implements CollectedTermsHolder {

	public String type;
	public DataGroup filter;
	public List<String> returnedIds;
	public boolean findRecordsForFilterWasCalled = false;
	public Map<String, List<String>> returnIdsForTypes = new HashMap<>();

	@Override
	public void storeCollectedTerms(String recordType, String recordId,
			List<StorageTerm> storageTerms, String dataDivider) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<String> findRecordIdsForFilter(String type, DataGroup filter) {
		findRecordsForFilterWasCalled = true;
		this.type = type;
		this.filter = filter;
		returnedIds = new ArrayList<>();
		if (returnIdsForTypes.isEmpty() || returnIdsForTypes.containsKey(type)) {
			returnedIds.addAll(returnIdsForTypes.get(type));
		}
		return returnedIds;
	}

	@Override
	public void removePreviousCollectedStorageTerms(String recordType, String recordId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void storeCollectedStorageTermData(String recordType, String storageKey, String recordId,
			StorageTermData storageTermData) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, DataGroup> structureCollectedTermsForDisk() {
		// TODO Auto-generated method stub
		return null;
	}

}
