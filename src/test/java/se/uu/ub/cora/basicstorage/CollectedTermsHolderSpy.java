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
import java.util.List;
import java.util.Map;

import se.uu.ub.cora.data.DataGroup;

public class CollectedTermsHolderSpy implements CollectedTermsHolder {

	public String type;
	public DataGroup filter;
	public List<String> returnedIds;
	public boolean findRecordsForFilterWasCalled = false;

	@Override
	public void storeCollectedTerms(String recordType, String recordId, DataGroup collectedTerms,
			String dataDivider) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<String> findRecordIdsForFilter(String type, DataGroup filter) {
		findRecordsForFilterWasCalled = true;
		this.type = type;
		this.filter = filter;
		returnedIds = new ArrayList<>();
		returnedIds.add("place:0002");
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