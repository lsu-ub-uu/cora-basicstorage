/*
 * Copyright 2017, 2018 Uppsala University Library
 * Copyright 2019 Olov McKie
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

import se.uu.ub.cora.apptokenstorage.AppTokenStorage;
import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.logger.Logger;
import se.uu.ub.cora.logger.LoggerProvider;
import se.uu.ub.cora.storage.RecordNotFoundException;

public class AppTokenStorageImp extends SecurityStorage implements AppTokenStorage {
	private Logger log = LoggerProvider.getLoggerForClass(AppTokenStorageImp.class);
	private Map<String, String> initInfo;

	public static AppTokenStorageImp usingInitInfo(Map<String, String> initInfo) {
		return new AppTokenStorageImp(initInfo);
	}

	private AppTokenStorageImp(Map<String, String> initInfo) {
		this.initInfo = initInfo;
		ensureInitInfoContainsStorageOnDiskBasePath(initInfo);
		basePath = initInfo.get("storageOnDiskBasePath");
		log.logInfoUsingMessage("Starting AppTokenStorageImp using basePath: " + basePath);
		populateFromStorage();
	}

	private final void ensureInitInfoContainsStorageOnDiskBasePath(Map<String, String> initInfo) {
		if (!initInfo.containsKey("storageOnDiskBasePath")) {
			String message = "initInfo must contain storageOnDiskBasePath";
			log.logFatalUsingMessage(message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public boolean userIdHasAppToken(String userId, String appToken) {
		if (populatedDataUserIdHasAppToken(userId, appToken)) {
			return true;
		}
		populateFromStorage();
		return populatedDataUserIdHasAppToken(userId, appToken);
	}

	private boolean populatedDataUserIdHasAppToken(String userId, String appToken) {
		List<String> appTokensForUser = findUserAndGetAppTokens(userId);
		return apptokenFoundInList(appToken, appTokensForUser);
	}

	private boolean apptokenFoundInList(String appToken, List<String> tokenList) {
		return tokenList.stream().anyMatch(token -> token.equals(appToken));
	}

	private List<String> findUserAndGetAppTokens(String userId) {
		DataGroup user = findUser(userId);
		return getAppTokensForUser(user);
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

	private List<String> getAppTokensForUser(DataGroup user) {
		if (userExistsAndIsActive(user)) {
			return getAppTokensForActiveUser(user);
		}
		return new ArrayList<>();
	}

	private boolean userExistsAndIsActive(DataGroup user) {
		return user != null && userIsActive(user);
	}

	private List<String> getAppTokensForActiveUser(DataGroup user) {
		List<DataGroup> userAppTokenGroups = user.getAllGroupsWithNameInData("userAppTokenGroup");
		return getAppTokensForAppTokenGroups(userAppTokenGroups);
	}

	private List<String> getAppTokensForAppTokenGroups(List<DataGroup> userAppTokenGroups) {
		List<String> apptokens = new ArrayList<>(userAppTokenGroups.size());
		for (DataGroup userAppTokenGroup : userAppTokenGroups) {
			String appTokenId = extractAppTokenId(userAppTokenGroup);
			apptokens.add(getTokenFromStorage(appTokenId));
		}
		return apptokens;
	}

	private String extractAppTokenId(DataGroup userAppTokenGroup) {
		return userAppTokenGroup.getFirstGroupWithNameInData("appTokenLink")
				.getFirstAtomicValueWithNameInData("linkedRecordId");
	}

	private String getTokenFromStorage(String appTokenId) {
		return recordStorage.read(List.of("appToken"), appTokenId)
				.getFirstAtomicValueWithNameInData("token");
	}

	private boolean userIsActive(DataGroup user) {
		return "active".equals(user.getFirstAtomicValueWithNameInData("activeStatus"));
	}

	public Map<String, String> getInitInfo() {
		// needed for test
		return initInfo;
	}

}
