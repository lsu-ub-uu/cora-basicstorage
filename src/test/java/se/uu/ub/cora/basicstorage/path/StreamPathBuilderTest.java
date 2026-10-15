/*
 * Copyright 2023 Uppsala University Library
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
package se.uu.ub.cora.basicstorage.path;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.function.Supplier;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.storage.StorageException;
import se.uu.ub.cora.storage.StreamPathBuilder;
import se.uu.ub.cora.storage.spies.hash.CoraDigestorSpy;

public class StreamPathBuilderTest {

	private static final String DATA_DIVIDER = "someDataDivider";
	private static final String TYPE = "someType";
	private static final String ID = "someId";
	private static final String FILE_SYSTEM_BASE_PATH = "/tmp/streamStorageOnDiskTempStream/";
	private static final String REPRESENTATION = "someRepresentation";

	private StreamPathBuilderImp pathBuilder;
	private CoraDigestorSpy digestor;
	private PathFileSystemSpy pathFileSystem;
	private int supplierCount = 0;

	@BeforeMethod
	private void beforeMethod() {
		setDigestorSpy();
		pathFileSystem = new PathFileSystemSpy();

		pathBuilder = StreamPathBuilderImp.usingBasePathAndCoraDigestorAndPathFileSystem(
				FILE_SYSTEM_BASE_PATH, digestor, pathFileSystem);
	}

	@AfterMethod
	public void afterMethod() {
		supplierCount = 0;
	}

	private void setDigestorSpy() {
		digestor = new CoraDigestorSpy();
		digestor.MRV.setDefaultReturnValuesSupplier("stringToSha256Hex", () -> "ABCDEFGHIJKLMNO");
	}

	@Test
	public void testPathBuilderImpImplementsPathBuilder() {
		assertTrue(pathBuilder instanceof StreamPathBuilder);
	}

	@Test
	public void testCallBuildPathToFileSystemAndEnsureExists_nonExistingBasePath() {
		pathFileSystem.MRV.setDefaultReturnValuesSupplier("notExists", () -> true);
		try {
			pathBuilder = StreamPathBuilderImp.usingBasePathAndCoraDigestorAndPathFileSystem(
					"/root/streamsDOESNOTEXIST", digestor, pathFileSystem);

			pathBuilder.buildPathToAFileAndEnsureFolderExists(DATA_DIVIDER, TYPE, ID,
					REPRESENTATION);
			fail("It should throw an exception");
		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertEquals(e.getMessage(),
					"File base path /root/streamsDOESNOTEXIST does not exists.");
		}
	}

	@Test
	public void testBuildPathToFile() {
		String pathAsString = pathBuilder.buildPathToAFile(DATA_DIVIDER, TYPE, ID, REPRESENTATION);

		digestor.MCR.assertParameters("stringToSha256Hex", 0, TYPE + ":" + ID);
		String folderPathAsString = "/tmp/streamStorageOnDiskTempStream/streams/someDataDivider"
				+ "/abc/def/ghi/abcdefghijklmno/";
		assertEquals(pathAsString, folderPathAsString + "someType:someId-someRepresentation");
	}

	@Test
	public void testCreatePathToFile() {
		String pathAsString = pathBuilder.buildPathToAFileAndEnsureFolderExists(DATA_DIVIDER, TYPE,
				ID, REPRESENTATION);

		assertEquals(pathAsString, "/tmp/streamStorageOnDiskTempStream/streams/someDataDivider"
				+ "/abc/def/ghi/abcdefghijklmno/someType:someId-someRepresentation");
		pathFileSystem.MCR.methodWasCalled("notExists");
		Path path = (Path) pathFileSystem.MCR
				.getParameterForMethodAndCallNumberAndParameter("notExists", 0, "path");
		assertEquals(path.toString(), "/tmp/streamStorageOnDiskTempStream");
		digestor.MCR.assertParameters("stringToSha256Hex", 0, TYPE + ":" + ID);
	}

	private void assertDirectoryExists() {
		pathFileSystem.MCR.assertMethodWasCalled("createDirectory");
	}

	@Test
	public void testFilesWithSameDataDividerAndTypeAndId_storedInSameFolder() {
		pathFileSystem.MRV.setDefaultReturnValuesSupplier("notExists",
				(Supplier<?>) this::customeSupplier);
		String pathAsString0 = pathBuilder.buildPathToAFileAndEnsureFolderExists(DATA_DIVIDER, TYPE,
				ID, "rep0");
		String pathAsString1 = pathBuilder.buildPathToAFileAndEnsureFolderExists(DATA_DIVIDER, TYPE,
				ID, "rep1");

		assertDirectoryExists();
		String sharedFolder = "/tmp/streamStorageOnDiskTempStream/streams/someDataDivider"
				+ "/abc/def/ghi/abcdefghijklmno/";
		assertTrue(pathAsString0.startsWith(sharedFolder));
		assertTrue(pathAsString1.startsWith(sharedFolder));

		assertTrue(pathAsString0.endsWith("someType:someId-rep0"));
		assertTrue(pathAsString1.endsWith("someType:someId-rep1"));
	}

	@Test
	public void testDifferentDataDivider_differentFolderButSameFileName() {
		pathFileSystem.MRV.setDefaultReturnValuesSupplier("notExists",
				(Supplier<?>) this::customeSupplier);

		String pathAsString0 = pathBuilder.buildPathToAFileAndEnsureFolderExists(DATA_DIVIDER, TYPE,
				ID, "rep0");
		String pathAsString1 = pathBuilder
				.buildPathToAFileAndEnsureFolderExists("someOtherDataDivider", TYPE, ID, "rep1");

		assertDirectoryExists();
		String sharedFolder = "/tmp/streamStorageOnDiskTempStream/streams/someDataDivider"
				+ "/abc/def/ghi/abcdefghijklmno/";
		assertTrue(pathAsString0.startsWith(sharedFolder));
		assertFalse(pathAsString1.startsWith(sharedFolder));

		assertTrue(pathAsString0.endsWith("someType:someId-rep0"));
		assertTrue(pathAsString1.endsWith("someType:someId-rep1"));
	}

	@Test
	public void testPermissions() {
		pathFileSystem.MRV.setDefaultReturnValuesSupplier("notExists",
				(Supplier<?>) this::customeSupplier);
		pathBuilder.buildPathToAFileAndEnsureFolderExists(DATA_DIVIDER, TYPE, ID, REPRESENTATION);

		assertAllPermissions(0, Paths.get(FILE_SYSTEM_BASE_PATH, "streams"));
		assertAllPermissions(1, Paths.get(FILE_SYSTEM_BASE_PATH, "streams", DATA_DIVIDER));
		assertAllPermissions(2, Paths.get(FILE_SYSTEM_BASE_PATH, "streams", DATA_DIVIDER, "abc"));
		assertAllPermissions(3,
				Paths.get(FILE_SYSTEM_BASE_PATH, "streams", DATA_DIVIDER, "abc", "def"));
		assertAllPermissions(4,
				Paths.get(FILE_SYSTEM_BASE_PATH, "streams", DATA_DIVIDER, "abc", "def", "ghi"));
		assertAllPermissions(5, Paths.get(FILE_SYSTEM_BASE_PATH, "streams", DATA_DIVIDER, "abc",
				"def", "ghi", "abcdefghijklmno"));

	}

	private Object customeSupplier() {
		supplierCount++;
		if (Math.floorMod(supplierCount, 2) == 1) {
			return false;
		}
		return true;
	}

	@Test
	public void testErrorWhileCreatingFolder() {
		pathFileSystem.MRV.setDefaultReturnValuesSupplier("notExists",
				(Supplier<?>) this::customeSupplier);
		pathFileSystem.MRV.setAlwaysThrowException("createDirectory", new RuntimeException());

		try {
			pathBuilder.buildPathToAFileAndEnsureFolderExists(DATA_DIVIDER, TYPE, ID,
					REPRESENTATION);

			fail("Must throw exception");
		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			String folderPathAsString = "/tmp/streamStorageOnDiskTempStream/streams/someDataDivider"
					+ "/abc/def/ghi/abcdefghijklmno";
			assertEquals(e.getMessage(),
					"Failed to create folder " + folderPathAsString + ", in filesystem");
		}
	}

	private void assertAllPermissions(int callNumber, Path path) {
		Set<PosixFilePermission> expectedPermissions = PosixFilePermissions.fromString("rwxrwxrwx");

		pathFileSystem.MCR.assertParameterAsEqual("setPosixFilePermissions", callNumber, "path",
				path);
		pathFileSystem.MCR.assertParameterAsEqual("setPosixFilePermissions", callNumber,
				"permissions", expectedPermissions);
	}

	@Test
	public void testOnlyForTests() {
		assertEquals(pathBuilder.onlyForTestGetFileSystemBasePath(), FILE_SYSTEM_BASE_PATH);
		assertEquals(pathBuilder.onlyForTestGetCoraDigestor(), digestor);
		assertEquals(pathBuilder.onlyForTestGetPathFileSystem(), pathFileSystem);
	}

}
