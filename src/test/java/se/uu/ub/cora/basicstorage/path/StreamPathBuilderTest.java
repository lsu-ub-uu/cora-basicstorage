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
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.stream.Stream;

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

	@BeforeMethod
	private void beforeMethod() throws Exception {
		makeSureBasePathExistsAndIsEmpty();
		setDigestorSpy();
		pathBuilder = StreamPathBuilderImp.usingBasePathAndCoraDigestor(FILE_SYSTEM_BASE_PATH,
				digestor);
	}

	@AfterMethod
	public void afterMethod() throws IOException {
		removeTempFiles();
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
		try {
			pathBuilder = StreamPathBuilderImp
					.usingBasePathAndCoraDigestor("/root/streamsDOESNOTEXIST", digestor);

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

		Path path = Paths.get(folderPathAsString);
		assertFalse(Files.exists(path));
	}

	@Test
	public void testCreatePathToFile() {
		String pathAsString = pathBuilder.buildPathToAFileAndEnsureFolderExists(DATA_DIVIDER, TYPE,
				ID, REPRESENTATION);

		digestor.MCR.assertParameters("stringToSha256Hex", 0, TYPE + ":" + ID);
		assertEquals(pathAsString, "/tmp/streamStorageOnDiskTempStream/streams/someDataDivider"
				+ "/abc/def/ghi/abcdefghijklmno/someType:someId-someRepresentation");
		assertDirectoryExists();
	}

	private void assertDirectoryExists() {
		Path path = Paths.get(FILE_SYSTEM_BASE_PATH, "streams", DATA_DIVIDER, "abc", "def", "ghi",
				"abcdefghijklmno");
		assertTrue(Files.exists(path));
	}

	@Test
	public void testFilesWithSameDataDividerAndTypeAndId_storedInSameFolder() {
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
	public void testPermissions() throws Exception {
		pathBuilder.buildPathToAFileAndEnsureFolderExists(DATA_DIVIDER, TYPE, ID, REPRESENTATION);

		assertAllPermissions(Paths.get(FILE_SYSTEM_BASE_PATH, "streams"));
		assertAllPermissions(Paths.get(FILE_SYSTEM_BASE_PATH, "streams", DATA_DIVIDER));
		assertAllPermissions(Paths.get(FILE_SYSTEM_BASE_PATH, "streams", DATA_DIVIDER, "abc"));
		assertAllPermissions(
				Paths.get(FILE_SYSTEM_BASE_PATH, "streams", DATA_DIVIDER, "abc", "def"));
		assertAllPermissions(
				Paths.get(FILE_SYSTEM_BASE_PATH, "streams", DATA_DIVIDER, "abc", "def", "ghi"));
		assertAllPermissions(Paths.get(FILE_SYSTEM_BASE_PATH, "streams", DATA_DIVIDER, "abc", "def",
				"ghi", "abcdefghijklmno"));

	}

	@Test
	public void testErrorWhileCreatingFolder() throws Exception {
		setPermissionToPath(Paths.get(FILE_SYSTEM_BASE_PATH), "r--r--r--");
		try {
			pathBuilder.buildPathToAFileAndEnsureFolderExists(DATA_DIVIDER, TYPE, ID,
					REPRESENTATION);
			fail();
		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			String folderPathAsString = "/tmp/streamStorageOnDiskTempStream/streams/someDataDivider"
					+ "/abc/def/ghi/abcdefghijklmno";
			assertEquals(e.getMessage(),
					"Failed to create folder " + folderPathAsString + ", in filesystem");
		}

		setPermissionToPath(Paths.get(FILE_SYSTEM_BASE_PATH), "rw-rw-rw-");
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
		assertEquals(pathBuilder.onlyForTestGetFileSystemBasePath(), FILE_SYSTEM_BASE_PATH);
		assertEquals(pathBuilder.onlyForTestGetCoraDigestor(), digestor);
	}

	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(FILE_SYSTEM_BASE_PATH);
		dir.mkdir();
		deleteFiles(FILE_SYSTEM_BASE_PATH);
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

	private void removeTempFiles() throws IOException {
		if (Files.exists(Paths.get(FILE_SYSTEM_BASE_PATH))) {
			deleteFiles(FILE_SYSTEM_BASE_PATH);
			File dir = new File(FILE_SYSTEM_BASE_PATH);
			dir.delete();
		}
	}

	private void setPermissionToPath(Path path, String permissions) throws IOException {
		Set<PosixFilePermission> posixPermissions = PosixFilePermissions.fromString(permissions);
		Files.setPosixFilePermissions(path, posixPermissions);
	}
}
