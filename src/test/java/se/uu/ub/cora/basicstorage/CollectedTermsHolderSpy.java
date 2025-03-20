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
import java.util.Set;

import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.storage.Filter;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;
import se.uu.ub.cora.testutils.mrv.MethodReturnValues;

public class CollectedTermsHolderSpy implements CollectedTermsHolder {

	public MethodCallRecorder MCR = new MethodCallRecorder();
	public MethodReturnValues MRV = new MethodReturnValues();

	public CollectedTermsHolderSpy() {
		MCR.useMRV(MRV);
		// MRV.setDefaultReturnValuesSupplier("findRecordIdsForFilter", () ->
		// Collections.EMPTY_LIST);
	}

	public String type;
	public Filter filter;
	public List<String> returnedIds;
	public boolean findRecordsForFilterWasCalled = false;
	public Map<String, List<String>> returnIdsForTypes = new HashMap<>();

	@Override
	public void storeCollectedTerms(String recordType, String recordId,
			Set<StorageTerm> storageTerms, String dataDivider) {
		MCR.addCall("recordType", recordType, "recordId", recordId, "storageTerms", storageTerms,
				"dataDivider", dataDivider);
	}

	@Override
	public List<String> findRecordIdsForFilter(String type, Filter filter) {
		MCR.addCall("type", type, "filter", filter);
		findRecordsForFilterWasCalled = true;
		this.type = type;
		this.filter = filter;
		returnedIds = new ArrayList<>();
		if (returnIdsForTypes.isEmpty() || returnIdsForTypes.containsKey(type)) {
			returnedIds.addAll(returnIdsForTypes.get(type));
		}
		MCR.addReturned(returnedIds);
		return returnedIds;
	}

	@Override
	public void removePreviousCollectedStorageTerms(String recordType, String recordId) {
		MCR.addCall("recordType", recordType, "recordId", recordId);
	}

	@Override
	public Set<StorageTerm> getCollectTerms(String recordType, String recordId) {
		// TODO Auto-generated method stub
		return null;
	}
}
