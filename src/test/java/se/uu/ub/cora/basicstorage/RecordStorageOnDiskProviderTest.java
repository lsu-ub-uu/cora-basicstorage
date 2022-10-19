/*
 * Copyright 2019 Uppsala University Library
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
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.basicstorage.log.LoggerFactorySpy;
import se.uu.ub.cora.initialize.SettingsProvider;
import se.uu.ub.cora.logger.LoggerProvider;
import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.storage.RecordStorageInstanceProvider;

public class RecordStorageOnDiskProviderTest {
	private Map<String, String> initInfo = new HashMap<>();
	private String basePath = "/tmp/recordStorageOnDiskTempBasicStorageProvider/";
	private LoggerFactorySpy loggerFactorySpy;
	private String testedClassName = "RecordStorageOnDiskProvider";
	private RecordStorageInstanceProvider recordStorageOnDiskProvider;

	@BeforeMethod
	public void beforeMethod() throws Exception {
		loggerFactorySpy = new LoggerFactorySpy();
		LoggerProvider.setLoggerFactory(loggerFactorySpy);

		initInfo = new HashMap<>();
		initInfo.put("storageOnDiskBasePath", basePath);
		initInfo.put("storageType", "disk");
		SettingsProvider.setSettings(initInfo);
		makeSureBasePathExistsAndIsEmpty();
		recordStorageOnDiskProvider = new RecordStorageOnDiskProvider();
		RecordStorageInstance.setInstance(null);
	}

	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(basePath);
		dir.mkdir();
		deleteFiles(basePath);

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

	@Test
	public void testGetOrderToSelectImplementationsByIsZero() {
		recordStorageOnDiskProvider.getRecordStorage();

		assertEquals(recordStorageOnDiskProvider.getOrderToSelectImplementionsBy(), 0);
	}

	@Test
	public void testNormalStartupReturnsRecordStorageOnDisk() {
		RecordStorage recordStorage = recordStorageOnDiskProvider.getRecordStorage();

		assertTrue(recordStorage instanceof RecordStorageOnDisk);
		assertFalse(recordStorage instanceof RecordStorageInMemoryReadFromDisk);
	}

	@Test
	public void testNormalStartupReturnsRecordStorageInMemoryReadFromDisk() {
		initInfo.put("storageType", "memory");

		recordStorageOnDiskProvider.getRecordStorage();

		RecordStorage recordStorage = recordStorageOnDiskProvider.getRecordStorage();
		assertTrue(recordStorage instanceof RecordStorageInMemoryReadFromDisk);
	}

	@Test
	public void testNormalStartupBasePathSentToRecordStorage() {
		RecordStorageOnDisk recordStorage = (RecordStorageOnDisk) recordStorageOnDiskProvider
				.getRecordStorage();

		assertEquals(recordStorage.getBasePath(), initInfo.get("storageOnDiskBasePath"));
	}

	@Test
	public void testNormalStartupReturnsTheSameRecordStorageForMultipleCalls() {
		RecordStorage recordStorage = recordStorageOnDiskProvider.getRecordStorage();
		RecordStorage recordStorage2 = recordStorageOnDiskProvider.getRecordStorage();

		assertSame(recordStorage, recordStorage2);
	}

	@Test
	public void testRecordStorageStartedByOtherProviderIsReturned() {
		RecordStorageSpy recordStorageSpy = new RecordStorageSpy();
		RecordStorageInstance.setInstance(recordStorageSpy);

		RecordStorage recordStorage = recordStorageOnDiskProvider.getRecordStorage();

		assertSame(recordStorage, recordStorageSpy);
	}

	@Test
	public void testRecordStorageIsAccessibleToOthers() {
		RecordStorage recordStorage = recordStorageOnDiskProvider.getRecordStorage();

		assertSame(recordStorage, RecordStorageInstance.getInstance());
	}

	@Test
	public void testLoggingNormalStartup() {
		recordStorageOnDiskProvider.getRecordStorage();

		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 0),
				"RecordStorageOnDiskProvider starting RecordStorageOnDisk...");
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 1),
				"RecordStorageOnDiskProvider started RecordStorageOnDisk");
		assertEquals(loggerFactorySpy.getNoOfInfoLogMessagesUsingClassName(testedClassName), 2);
	}

	@Test
	public void testLoggingAndErrorIfMissingStartParameters() {
		initInfo.remove("storageOnDiskBasePath");
		try {
			recordStorageOnDiskProvider.getRecordStorage();
		} catch (Exception e) {

		}
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 0),
				"RecordStorageOnDiskProvider starting RecordStorageOnDisk...");
		assertEquals(loggerFactorySpy.getNoOfInfoLogMessagesUsingClassName(testedClassName), 1);
	}

}
