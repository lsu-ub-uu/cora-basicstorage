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

import java.io.File;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.storage.StreamPathBuilder;

public class StreamPathBuilderTest {

	private static final String SOME_DATA_DIVIDER = "someDataDivider";
	private static final String SOME_TYPE = "someType";
	private static final String SOME_ID = "someId";
	private static final String SOME_FILE_SYSTEM_BASE_PATH = "/tmp/streamStorageOnDiskTempStream/";

	private StreamPathBuilderImp pathBuilder;

	@BeforeMethod
	private void beforeMethod() throws Exception {
		makeSureBasePathExistsAndIsEmpty();
		pathBuilder = new StreamPathBuilderImp(SOME_FILE_SYSTEM_BASE_PATH);
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
	public void testPathBuilderImpImplementsPathBuilder() throws Exception {
		assertTrue(pathBuilder instanceof StreamPathBuilder);
	}

	@Test
	public void testCallBuildPathToFileSystemAndEnsureExistsCHANGE() throws IOException {
		Exception caughtException = null;
		try {
			removeTempFiles();
			pathBuilder = new StreamPathBuilderImp("/root/streamsDOESNOTEXIST");

			pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER, SOME_TYPE,
					SOME_ID);
		} catch (Exception e) {
			caughtException = e;
		}
		assertTrue(caughtException.getCause() instanceof AccessDeniedException);
		assertEquals(caughtException.getMessage(), "can not write files to disk: "
				+ "java.nio.file.AccessDeniedException: /root/streamsDOESNOTEXIST/streams");
	}

	@Test
	public void testInitMissingPath() throws IOException {
		pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER, SOME_TYPE, SOME_ID);

		assertTrue(
				Files.exists(Paths.get(SOME_FILE_SYSTEM_BASE_PATH, "streams", "someDataDivider")));
	}

	@Test
	public void testInitPathMoreThanOnce() throws IOException {
		pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER, SOME_TYPE, SOME_ID);
		pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER, SOME_TYPE, SOME_ID);
		assertTrue(
				Files.exists(Paths.get(SOME_FILE_SYSTEM_BASE_PATH, "streams", "someDataDivider")));
	}

	@Test
	public void testCallBuildPathToFileSystemAndEnsureExists() throws Exception {
		String pathToAFileInFileSystem = pathBuilder
				.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER, SOME_TYPE, SOME_ID);

		assertEquals(pathToAFileInFileSystem, SOME_FILE_SYSTEM_BASE_PATH + "streams/"
				+ SOME_DATA_DIVIDER + "/" + SOME_TYPE + ":" + SOME_ID);
	}

	@Test
	public void testPermissions() throws Exception {
		pathBuilder.buildPathToAFileAndEnsureFolderExists(SOME_DATA_DIVIDER, SOME_TYPE, SOME_ID);

		assertAllPermissions(Paths.get(SOME_FILE_SYSTEM_BASE_PATH, "streams"));
		assertAllPermissions(Paths.get(SOME_FILE_SYSTEM_BASE_PATH, "streams", "someDataDivider"));
	}

	private void assertAllPermissions(Path path) throws IOException {
		assertTruePermissions(path, PosixFilePermission.OWNER_READ);
		assertTruePermissions(path, PosixFilePermission.OWNER_WRITE);
		assertTruePermissions(path, PosixFilePermission.OWNER_EXECUTE);
		assertTruePermissions(path, PosixFilePermission.GROUP_WRITE);
		assertTruePermissions(path, PosixFilePermission.GROUP_WRITE);
		assertTruePermissions(path, PosixFilePermission.GROUP_EXECUTE);
		assertTruePermissions(path, PosixFilePermission.OTHERS_READ);
		assertFalsePermissions(path, PosixFilePermission.OTHERS_WRITE);
		assertTruePermissions(path, PosixFilePermission.OTHERS_EXECUTE);
	}

	private void assertTruePermissions(Path path, PosixFilePermission permission)
			throws IOException {
		Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
		assertTrue(permissions.contains(permission));
	}

	private void assertFalsePermissions(Path path, PosixFilePermission permission)
			throws IOException {
		Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
		assertFalse(permissions.contains(permission));
	}

}
