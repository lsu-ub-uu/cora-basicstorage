/*
 * Copyright 2017 Uppsala University Library
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
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
import se.uu.ub.cora.basicstorage.log.LoggerFactorySpy;
import se.uu.ub.cora.basicstorage.testdata.TestDataAppTokenStorage;
import se.uu.ub.cora.data.DataAtomicFactory;
import se.uu.ub.cora.data.DataAtomicProvider;
import se.uu.ub.cora.data.DataGroupFactory;
import se.uu.ub.cora.data.DataGroupProvider;
import se.uu.ub.cora.data.converter.DataToJsonConverterFactory;
import se.uu.ub.cora.data.converter.DataToJsonConverterProvider;
import se.uu.ub.cora.data.converter.JsonToDataConverterFactory;
import se.uu.ub.cora.data.converter.JsonToDataConverterProvider;
import se.uu.ub.cora.data.copier.DataCopierFactory;
import se.uu.ub.cora.data.copier.DataCopierProvider;
import se.uu.ub.cora.logger.LoggerProvider;

public class AppTokenStorageTest {
	private String basePath = "/tmp/recordStorageOnDiskTempApptoken/";
	private Map<String, String> initInfo;
	private AppTokenStorageImp appTokenStorage;
	private LoggerFactorySpy loggerFactorySpy;
	private String testedClassName = "AppTokenStorageImp";
	private DataGroupFactory dataGroupFactory;
	private DataCopierFactory dataCopierFactory;
	private DataAtomicFactory dataAtomicFactory;
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
		appTokenStorage = AppTokenStorageImp.usingInitInfo(initInfo);
	}

	private void setUpFactoriesAndProviders() {
		loggerFactorySpy = new LoggerFactorySpy();
		LoggerProvider.setLoggerFactory(loggerFactorySpy);
		dataGroupFactory = new DataGroupFactorySpy();
		DataGroupProvider.setDataGroupFactory(dataGroupFactory);
		dataCopierFactory = new DataCopierFactorySpy();
		DataCopierProvider.setDataCopierFactory(dataCopierFactory);
		dataAtomicFactory = new DataAtomicFactorySpy();
		DataAtomicProvider.setDataAtomicFactory(dataAtomicFactory);
		jsonToDataConverterFactory = new JsonToDataConverterFactoryImp();
		JsonToDataConverterProvider.setJsonToDataConverterFactory(jsonToDataConverterFactory);
		BasicDataToJsonConverterFactoryCreator factoryCreator = new BasicDataToJsonConverterFactoryCreator();
		DataToJsonConverterProvider.setDataToJsonConverterFactoryCreator(factoryCreator);
		// dataToJsonConverterFactory = new DataToJsonConverterFactoryImp();
		// DataToJsonConverterProvider.setDataToJsonConverterFactory(dataToJsonConverterFactory);
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
		assertTrue(appTokenStorage.recordStorage instanceof RecordStorageInMemoryReadFromDisk);
	}

	@Test
	public void testStartupLogsInfoAboutUsedPath() throws Exception {
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 0),
				"Starting AppTokenStorageImp using basePath: " + basePath);
	}

	@Test
	public void testInitNoStorageOnDiskBasePath() {
		initInfo = new HashMap<>();
		Exception excepiton = startAppTokenVerifierMakeSureAnExceptionIsThrown();
		assertEquals(excepiton.getMessage(), "initInfo must contain storageOnDiskBasePath");
		String fatalMessage = loggerFactorySpy
				.getFatalLogMessageUsingClassNameAndNo(testedClassName, 0);
		assertEquals(fatalMessage, "initInfo must contain storageOnDiskBasePath");
	}

	private Exception startAppTokenVerifierMakeSureAnExceptionIsThrown() {
		Exception caughtException = null;
		try {
			AppTokenStorageImp.usingInitInfo(initInfo);
		} catch (Exception e) {
			caughtException = e;
		}
		assertTrue(caughtException instanceof RuntimeException);
		assertNotNull(caughtException);
		return caughtException;
	}

	@Test
	public void testGetAppTokenForUser() {
		assertTrue(appTokenStorage.userIdHasAppToken("dummy1", "someSecretString"));
		assertEquals(appTokenStorage.getNoOfReadsFromDisk(), 1);
	}

	@Test
	public void testGetAppTokenForUser2() {
		assertTrue(appTokenStorage.userIdHasAppToken("dummy2", "someOtherSecretString"));
	}

	@Test
	public void testGetAppTokenForNonExistingUser() {
		assertFalse(appTokenStorage.userIdHasAppToken("nonExistingUser", "someThirdSecretString"));
	}

	@Test
	public void testGetAppTokenForInactiveUser() {
		assertFalse(appTokenStorage.userIdHasAppToken("inactiveUser", "someThirdSecretString"));
	}

	@Test
	public void testGetAppTokenForUserNoAppToken() {
		assertFalse(appTokenStorage.userIdHasAppToken("noAppTokenUser", "someThirdSecretString"));
	}

	@Test
	public void testGetAppTokenForUserNoAppTokenRepopulatesOnceFromDisk() {
		assertFalse(appTokenStorage.userIdHasAppToken("noAppTokenUser", "someThirdSecretString"));
		assertEquals(appTokenStorage.getNoOfReadsFromDisk(), 2);
	}

}
