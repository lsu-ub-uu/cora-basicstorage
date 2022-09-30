/*
 * Copyright 2017, 2018 Uppsala University Library
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
import java.util.List;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.DataGroupProvider;

public abstract class SecurityStorage {

	protected static final String RECORD_TYPE = "recordType";
	private static final String PARENT_ID = "parentId";
	protected RecordStorageInMemoryReadFromDisk recordStorage;
	protected List<String> userRecordTypeNames = new ArrayList<>();
	protected String basePath;
	private int noOfReadsFromDisk = 0;

	protected void populateFromStorage() {
		noOfReadsFromDisk++;
		recordStorage = RecordStorageInMemoryReadFromDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		populateUserRecordTypeNameList();
	}

	private void populateUserRecordTypeNameList() {
		Collection<DataGroup> recordTypes = recordStorage.readList(List.of(RECORD_TYPE),
				DataGroupProvider.getDataGroupUsingNameInData("filter")).listOfDataGroups;

		for (DataGroup recordTypePossibleChild : recordTypes) {
			addChildOfUserToUserRecordTypeNameList(recordTypePossibleChild);
		}
	}

	private void addChildOfUserToUserRecordTypeNameList(DataGroup recordTypePossibleChild) {
		if (isChildOfUserRecordType(recordTypePossibleChild)) {
			addChildToReadRecordList(recordTypePossibleChild);
		}
	}

	private boolean isChildOfUserRecordType(DataGroup recordTypePossibleChild) {
		return recordHasParent(recordTypePossibleChild)
				&& userRecordTypeIsParentToRecord(recordTypePossibleChild);
	}

	private void addChildToReadRecordList(DataGroup recordTypePossibleChild) {
		String childRecordType = recordTypePossibleChild.getFirstGroupWithNameInData("recordInfo")
				.getFirstAtomicValueWithNameInData("id");
		userRecordTypeNames.add(childRecordType);
	}

	protected boolean recordHasParent(DataGroup handledRecordTypeDataGroup) {
		return handledRecordTypeDataGroup.containsChildWithNameInData(PARENT_ID);
	}

	protected boolean userRecordTypeIsParentToRecord(DataGroup recordTypePossibleChild) {
		DataGroup parent = recordTypePossibleChild.getFirstGroupWithNameInData(PARENT_ID);
		return "user".equals(parent.getFirstAtomicValueWithNameInData("linkedRecordId"));
	}

	protected int getNoOfReadsFromDisk() {
		return noOfReadsFromDisk;
	}

}