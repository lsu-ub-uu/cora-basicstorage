/*
 * Copyright 2017 Uppsala University Library
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.basicdata.converter.datatojson.BasicDataToJsonConverterFactoryCreator;
import se.uu.ub.cora.basicdata.converter.jsontodata.JsonToDataConverterFactoryImp;
import se.uu.ub.cora.basicstorage.testdata.TestDataAppTokenStorage;
import se.uu.ub.cora.data.DataAtomicFactory;
import se.uu.ub.cora.data.DataAtomicProvider;
import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.DataGroupFactory;
import se.uu.ub.cora.data.DataGroupProvider;
import se.uu.ub.cora.data.converter.DataToJsonConverterFactory;
import se.uu.ub.cora.data.converter.DataToJsonConverterProvider;
import se.uu.ub.cora.data.converter.JsonToDataConverterFactory;
import se.uu.ub.cora.data.converter.JsonToDataConverterProvider;
import se.uu.ub.cora.data.copier.DataCopierFactory;
import se.uu.ub.cora.data.copier.DataCopierProvider;
import se.uu.ub.cora.storage.RecordNotFoundException;

public class UserStorageTest {
	private String basePath = "/tmp/recordStorageOnDiskTempUser/";
	private Map<String, String> initInfo;
	private UserStorageImp userStorage;
	private DataGroupFactory dataGroupFactory;
	private DataAtomicFactory dataAtomicFactory;
	private DataCopierFactory dataCopierFactory;
	private DataToJsonConverterFactory dataToJsonConverterFactory;
	private JsonToDataConverterFactory jsonToDataConverterFactory;

	@BeforeMethod
	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		setUpFactoriesAndProviders();
		File dir = new File(basePath);
		dir.mkdir();
		deleteFiles(basePath);
		TestDataAppTokenStorage.createRecordStorageInMemoryWithTestData(basePath);

		initInfo = new HashMap<>();
		initInfo.put("storageOnDiskBasePath", basePath);
		userStorage = new UserStorageImp(initInfo);
	}

	private void setUpFactoriesAndProviders() {
		dataGroupFactory = new DataGroupFactorySpy();
		DataGroupProvider.setDataGroupFactory(dataGroupFactory);
		dataAtomicFactory = new DataAtomicFactorySpy();
		DataAtomicProvider.setDataAtomicFactory(dataAtomicFactory);
		dataCopierFactory = new DataCopierFactorySpy();
		DataCopierProvider.setDataCopierFactory(dataCopierFactory);
		// dataToJsonConverterFactory = new DataToJsonConverterFactoryImp();
		// dataToJsonConverterFactory = new DataToJsonConverterFactorySpy();
		// DataToJsonConverterProvider.setDataToJsonConverterFactory(dataToJsonConverterFactory);
		BasicDataToJsonConverterFactoryCreator factoryCreator = new BasicDataToJsonConverterFactoryCreator();
		DataToJsonConverterProvider.setDataToJsonConverterFactoryCreator(factoryCreator);
		// jsonToDataConverterFactory = new JsonToDataConverterFactorySpy();
		jsonToDataConverterFactory = new JsonToDataConverterFactoryImp();
		JsonToDataConverterProvider.setJsonToDataConverterFactory(jsonToDataConverterFactory);
	}

	private void deleteFiles(String path) throws IOException {
		Stream<Path> list;
		list = Files.list(Paths.get(path));

		list.forEach(p -> deleteFile(p));
		list.close();
	}

	private void deleteFile(Path path) {
		try {
			if (path.toFile().isDirectory()) {
				deleteFiles(path.toString());
			}
			Files.delete(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@AfterMethod
	public void removeTempFiles() throws IOException {
		if (Files.exists(Paths.get(basePath))) {
			deleteFiles(basePath);
			File dir = new File(basePath);
			dir.delete();
		}
	}

	@Test
	public void test() {
		assertTrue(userStorage.recordStorage instanceof RecordStorageInMemoryReadFromDisk);
	}

	@Test(expectedExceptions = RuntimeException.class)
	public void testInitNoStorageOnDiskBasePath() {
		Map<String, String> initInfo = new HashMap<>();
		new UserStorageImp(initInfo);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testGetUserNotFound() {
		userStorage.getUserById("ThisUserDoesNotExist");
	}

	@Test
	public void testGetInitInfo() throws Exception {
		assertEquals(userStorage.getInitInfo(), initInfo);
	}

	@Test
	public void testGetUserDummy1() {
		DataGroup userGroup = userStorage.getUserById("dummy1");
		assertEquals(userGroup.getFirstGroupWithNameInData("recordInfo")
				.getFirstAtomicValueWithNameInData("id"), "dummy1");
	}

	@Test
	public void testGetUserRepopulatesOnceFromDiskIfNotFound() {
		RecordStorageOnDisk recordsOnDisk = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		TestDataAppTokenStorage.createUserOnDisk(recordsOnDisk);
		DataGroup userGroup = userStorage.getUserById("createdLater");
		assertEquals(userGroup.getFirstGroupWithNameInData("recordInfo")
				.getFirstAtomicValueWithNameInData("id"), "createdLater");
		assertEquals(userStorage.getNoOfReadsFromDisk(), 2);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testGetUserByIdFromLoginNotFound() {
		userStorage.getUserByIdFromLogin("NotInStorage@not.ever");
	}

	@Test
	public void testGetUserByIdFromLoginRepopulatesOnceFromDiskIfNotFound() {
		RecordStorageOnDisk recordsOnDisk = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		TestDataAppTokenStorage.createUserOnDisk(recordsOnDisk);
		DataGroup userGroup = userStorage.getUserByIdFromLogin("createdLater@ub.uu.se");
		assertEquals(userGroup.getFirstGroupWithNameInData("recordInfo")
				.getFirstAtomicValueWithNameInData("id"), "createdLater");
		assertEquals(userStorage.getNoOfReadsFromDisk(), 2);
	}

	@Test
	public void testGetUserByIdFromLogin() {
		DataGroup userGroup = userStorage.getUserByIdFromLogin("noAppTokenUser@ub.uu.se");
		assertEquals(userGroup.getFirstGroupWithNameInData("recordInfo")
				.getFirstAtomicValueWithNameInData("id"), "noAppTokenUser");
	}

	@Test()
	public void testGetUserByIdFromLoginTwoUsersSameUserIdFilterShouldResultInUserNotFound() {
		String errorMessage = null;
		try {
			userStorage.getUserByIdFromLogin("sameUser@ub.uu.se");
		} catch (Exception e) {
			errorMessage = e.getMessage();
		}
		assertEquals(errorMessage,
				"More than one users with same userId, no user returned: sameUser@ub.uu.se");
	}

}
