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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.storage.StorageException;
import se.uu.ub.cora.storage.StreamPathBuilder;
import se.uu.ub.cora.storage.spies.hash.CoraDigestUtilsSpy;

public class StreamPathBuilderTest {

	private static final String SOME_DATA_DIVIDER = "someDataDivider";
	private static final String SOME_TYPE = "someType";
	private static final String SOME_ID = "someId";
	private static final String SOME_FILE_SYSTEM_BASE_PATH = "/tmp/streamStorageOnDiskTempStream/";
	private static final String SOME_REPRESENTATION = "someRepresentation";

	private StreamPathBuilderImp pathBuilder;
	private CoraDigestUtilsSpy digestor;

	@BeforeMethod
	private void beforeMethod() throws Exception {
		makeSureBasePathExistsAndIsEmpty();
		setDigestorSpy();
		pathBuilder = StreamPathBuilderImp
				.usingBasePathAndCoraDigestUtils(SOME_FILE_SYSTEM_BASE_PATH, digestor);
	}

	private void setDigestorSpy() {
		digestor = new CoraDigestUtilsSpy();
		digestor.MRV.setDefaultReturnValuesSupplier("sha256Hex", () -> "00000000000000000");
		digestor.MRV.setSpecificReturnValuesSupplier("sha256Hex", () -> "0123456789asdfghjkl",
				SOME_TYPE + ":" + SOME_ID);
	}

	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(SOME_FILE_SYSTEM_BASE_PATH);
		dir.mkdir();
		deleteFiles(SOME_FILE_SYSTEM_BASE_PATH);
	}

	private void deleteFiles(String path) throws IOException {
		Stream<Path> list;
		list = Files.list(Paths.get(path));
		list.forEach(p -> {
			try {
				deleteFile(p);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		list.close();
	}

	private void deleteFile(Path path) throws IOException {
		if (new File(path.toString()).isDirectory()) {
			deleteFiles(path.toString());
		}
		try {
			Files.delete(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@AfterMethod
	public void removeTempFiles() throws IOException {
		if (Files.exists(Paths.get(SOME_FILE_SYSTEM_BASE_PATH))) {
			deleteFiles(SOME_FILE_SYSTEM_BASE_PATH);
			File dir = new File(SOME_FILE_SYSTEM_BASE_PATH);
			dir.delete();
		}
	}

	@Test
	public void testPathBuilderImpImplementsPathBuilder() {
		assertTrue(pathBuilder instanceof StreamPathBuilder);
	}

	@Test
	public void testCallBuildPathToFileSystemAndEnsureExistsCHANGE() throws IOException {
		try {
			removeTempFiles();
			pathBuilder = StreamPathBuilderImp
					.usingBasePathAndCoraDigestUtils("/root/streamsDOESNOTEXIST", digestor);

			pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER, SOME_TYPE, SOME_ID,
					SOME_REPRESENTATION);
			fail("It should throw an exception");
		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertEquals(e.getMessage(),
					"File base path /root/streamsDOESNOTEXIST does not exists.");
		}
	}

	@Test
	public void testCreatePathToFile() {
		String pathAsString = pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER,
				SOME_TYPE, SOME_ID, SOME_REPRESENTATION);

		assertEquals(pathAsString, "/tmp/streamStorageOnDiskTempStream/streams/someDataDivider"
				+ "/012/345/678/0123456789asdfghjkl/someType:someId-someRepresentation");
		assertDirectoryExists();
	}

	private void assertDirectoryExists() {
		Path path = Paths.get(SOME_FILE_SYSTEM_BASE_PATH, "streams", SOME_DATA_DIVIDER, "012",
				"345", "678", "0123456789asdfghjkl");
		assertTrue(Files.exists(path));
	}

	@Test
	public void testFilesWithSameDataDividerAndTypeAndId_storedInSameFolder() {
		String pathAsString0 = pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER,
				SOME_TYPE, SOME_ID, "rep0");
		String pathAsString1 = pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER,
				SOME_TYPE, SOME_ID, "rep1");

		assertDirectoryExists();
		String sharedFolder = "/tmp/streamStorageOnDiskTempStream/streams/someDataDivider"
				+ "/012/345/678/0123456789asdfghjkl/";
		assertTrue(pathAsString0.startsWith(sharedFolder));
		assertTrue(pathAsString1.startsWith(sharedFolder));

		assertTrue(pathAsString0.endsWith("someType:someId-rep0"));
		assertTrue(pathAsString1.endsWith("someType:someId-rep1"));
	}

	@Test
	public void testDifferentDataDivider_differentFolderButSameFileName() {
		String pathAsString0 = pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER,
				SOME_TYPE, SOME_ID, "rep0");
		String pathAsString1 = pathBuilder.buildPathToAFileAndEnsureFolderExists(
				"someOtherDataDivider", SOME_TYPE, SOME_ID, "rep1");

		assertDirectoryExists();
		String sharedFolder = "/tmp/streamStorageOnDiskTempStream/streams/someDataDivider"
				+ "/012/345/678/0123456789asdfghjkl/";
		assertTrue(pathAsString0.startsWith(sharedFolder));
		assertFalse(pathAsString1.startsWith(sharedFolder));

		assertTrue(pathAsString0.endsWith("someType:someId-rep0"));
		assertTrue(pathAsString1.endsWith("someType:someId-rep1"));
	}

	@Test
	public void testDifferentType_differentFolderAndDifferentFileName() {
		String pathAsString0 = pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER,
				SOME_TYPE, SOME_ID, "rep0");
		String pathAsString1 = pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER,
				"someOther", SOME_ID, "rep1");

		assertDirectoryExists();
		String sharedFolder = "/tmp/streamStorageOnDiskTempStream/streams/someDataDivider"
				+ "/012/345/678/0123456789asdfghjkl/";
		assertTrue(pathAsString0.startsWith(sharedFolder));
		assertFalse(pathAsString1.startsWith(sharedFolder));

		assertTrue(pathAsString0.endsWith("someType:someId-rep0"));
		assertFalse(pathAsString1.endsWith("someType:someId-rep1"));
	}

	@Test
	public void testDifferentId_differentFolderAndDifferentFileName() {
		String pathAsString0 = pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER,
				SOME_TYPE, SOME_ID, "rep0");
		String pathAsString1 = pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER,
				SOME_TYPE, "someOtherId", "rep1");

		assertDirectoryExists();
		String sharedFolder = "/tmp/streamStorageOnDiskTempStream/streams/someDataDivider"
				+ "/012/345/678/0123456789asdfghjkl/";
		assertTrue(pathAsString0.startsWith(sharedFolder));
		assertFalse(pathAsString1.startsWith(sharedFolder));

		assertTrue(pathAsString0.endsWith("someType:someId-rep0"));
		assertFalse(pathAsString1.endsWith("someType:someId-rep1"));
	}

	@Test
	public void testPermissions() throws Exception {
		pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER, SOME_TYPE, SOME_ID,
				SOME_REPRESENTATION);

		assertAllPermissions(Paths.get(SOME_FILE_SYSTEM_BASE_PATH, "streams"));
		assertAllPermissions(Paths.get(SOME_FILE_SYSTEM_BASE_PATH, "streams", SOME_DATA_DIVIDER));
		assertAllPermissions(
				Paths.get(SOME_FILE_SYSTEM_BASE_PATH, "streams", SOME_DATA_DIVIDER, "012"));
		assertAllPermissions(
				Paths.get(SOME_FILE_SYSTEM_BASE_PATH, "streams", SOME_DATA_DIVIDER, "012", "345"));
		assertAllPermissions(Paths.get(SOME_FILE_SYSTEM_BASE_PATH, "streams", SOME_DATA_DIVIDER,
				"012", "345", "678"));
		assertAllPermissions(Paths.get(SOME_FILE_SYSTEM_BASE_PATH, "streams", SOME_DATA_DIVIDER,
				"012", "345", "678", "0123456789asdfghjkl"));

	}

	private void assertAllPermissions(Path path) throws IOException {
		assertTruePermissions(path, PosixFilePermission.OWNER_READ);
		assertTruePermissions(path, PosixFilePermission.OWNER_WRITE);
		assertTruePermissions(path, PosixFilePermission.OWNER_EXECUTE);
		assertTruePermissions(path, PosixFilePermission.GROUP_WRITE);
		assertTruePermissions(path, PosixFilePermission.GROUP_WRITE);
		assertTruePermissions(path, PosixFilePermission.GROUP_EXECUTE);
		assertTruePermissions(path, PosixFilePermission.OTHERS_READ);
		assertTruePermissions(path, PosixFilePermission.OTHERS_WRITE);
		assertTruePermissions(path, PosixFilePermission.OTHERS_EXECUTE);
	}

	private void assertTruePermissions(Path path, PosixFilePermission permission)
			throws IOException {
		Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
		assertTrue(permissions.contains(permission));
	}

	@Test
	public void testOnlyForTests() {
		assertEquals(pathBuilder.onlyForTestGetFileSystemBasePath(), SOME_FILE_SYSTEM_BASE_PATH);
		assertEquals(pathBuilder.onlyForTestGetCoraDigestorUtils(), digestor);
	}
}
