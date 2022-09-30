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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import se.uu.ub.cora.data.DataAtomicProvider;
import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.DataGroupProvider;
import se.uu.ub.cora.gatekeeper.user.UserStorage;
import se.uu.ub.cora.storage.RecordNotFoundException;

public class UserStorageImp extends SecurityStorage implements UserStorage {

	private Map<String, String> initInfo;

	public UserStorageImp(Map<String, String> initInfo) {
		this.initInfo = initInfo;
		if (!initInfo.containsKey("storageOnDiskBasePath")) {
			throw new RuntimeException("initInfo must contain storageOnDiskBasePath");
		}
		basePath = initInfo.get("storageOnDiskBasePath");
		populateFromStorage();
	}

	@Override
	public DataGroup getUserById(String id) {
		DataGroup userDataGroup = findUser(id);
		if (userDataGroup != null) {
			return userDataGroup;
		}
		return repopulateDataFromStorageAndGetUser(id);
	}

	private DataGroup findUser(String userId) {
		DataGroup user = null;
		for (String userRecordTypeName : userRecordTypeNames) {
			try {
				user = recordStorage.read(List.of(userRecordTypeName), userId);
			} catch (RecordNotFoundException e) {
				// do nothing
			}
		}
		return user;
	}

	private DataGroup repopulateDataFromStorageAndGetUser(String userId) {
		populateFromStorage();
		DataGroup findUser = findUser(userId);
		if (null == findUser) {
			throw new RecordNotFoundException("User not found: " + userId);
		}
		return findUser;
	}

	@Override
	public DataGroup getUserByIdFromLogin(String idFromLogin) {
		try {
			return findUserByIdFromLogin(idFromLogin);
		} catch (Exception e) {
			populateFromStorage();
		}
		return tryAfterRepopulateFromStorageToFindNewlyCreatedUsers(idFromLogin);
	}

	private DataGroup findUserByIdFromLogin(String idFromLogin) {
		Collection<DataGroup> foundUsers = getUserFromStorageByIdFromLogin(idFromLogin);
		throwErrorIfMoreThanOneUserReturnedFromStorage(idFromLogin, foundUsers);
		return foundUsers.iterator().next();
	}

	private Collection<DataGroup> getUserFromStorageByIdFromLogin(String idFromLogin) {
		DataGroup filter = createFilterForIdFromLogin(idFromLogin);
		return recordStorage.readAbstractList("user", filter).listOfDataGroups;
	}

	private void throwErrorIfMoreThanOneUserReturnedFromStorage(String idFromLogin,
			Collection<DataGroup> foundUsers) {
		if (foundUsers.size() > 1) {
			throw new RecordNotFoundException(
					"More than one users with same userId, no user returned: " + idFromLogin);
		}
	}

	private DataGroup createFilterForIdFromLogin(String idFromLogin) {
		DataGroup filter = DataGroupProvider.getDataGroupUsingNameInData("filter");
		DataGroup part = DataGroupProvider.getDataGroupUsingNameInData("part");
		filter.addChild(part);
		part.addChild(DataAtomicProvider.getDataAtomicUsingNameInDataAndValue("key", "userId"));
		part.addChild(
				DataAtomicProvider.getDataAtomicUsingNameInDataAndValue("value", idFromLogin));
		return filter;
	}

	private DataGroup tryAfterRepopulateFromStorageToFindNewlyCreatedUsers(String idFromLogin) {
		return findUserByIdFromLogin(idFromLogin);
	}

	public Map<String, String> getInitInfo() {
		return initInfo;
	}
}
