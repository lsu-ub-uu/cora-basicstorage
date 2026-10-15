/*
 * Copyright 2026 Uppsala University Library
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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PathFileSystemTest {

	private static final String FILE_SYSTEM_BASE_PATH = "/tmp/pathFileSystemTest/";
	private PathFileSystemImp pathFileSystem;

	@BeforeMethod
	public void beforeMethod() throws Exception {
		makeSureBasePathExistsAndIsEmpty();
		pathFileSystem = PathFileSystemImp.usingRealFileSystem();
	}

	@AfterMethod
	public void afterMethod() throws Exception {
		removeTempFiles();
	}

	@Test
	public void testNotExistsForMissingPath() {
		Path missingPath = Paths.get(FILE_SYSTEM_BASE_PATH, "missing");

		assertTrue(pathFileSystem.notExists(missingPath));
	}

	@Test
	public void testNotExistsForExistingPath() throws IOException {
		Path existingPath = Paths.get(FILE_SYSTEM_BASE_PATH, "existing");
		Files.createDirectory(existingPath);

		assertFalse(pathFileSystem.notExists(existingPath));
	}

	@Test
	public void testCreateDirectory() {
		Path pathToCreate = Paths.get(FILE_SYSTEM_BASE_PATH, "createdDir");

		pathFileSystem.createDirectory(pathToCreate);

		assertTrue(Files.exists(pathToCreate));
		assertTrue(Files.isDirectory(pathToCreate));
	}

	@Test
	public void testTCreateDirectoryAlreadyExists() throws IOException {
		Path pathToCreate = Paths.get(FILE_SYSTEM_BASE_PATH, "createdDir");
		Files.createDirectory(pathToCreate);

		pathFileSystem.createDirectory(pathToCreate);

		assertTrue(Files.exists(pathToCreate));
		assertTrue(Files.isDirectory(pathToCreate));
	}

	@Test
	public void testCreateDirectoryWithMissingParent_preservesOriginalCause() {
		Path missingParent = Paths.get(FILE_SYSTEM_BASE_PATH, "missingParent");
		Path directoryToCreate = missingParent.resolve("child");

		try {
			pathFileSystem.createDirectory(directoryToCreate);
			fail("Must throw exception");
		} catch (Exception e) {
			assertEquals(e.getMessage(),
					"Could not create directory /tmp/pathFileSystemTest/missingParent/child");
			assertNotNull(e.getCause());
			assertTrue(e.getCause() instanceof NoSuchFileException);
		}
	}

	@Test
	public void testSetPosixFilePermissions() throws Exception {
		Path path = Paths.get(FILE_SYSTEM_BASE_PATH, "dirWithPermissions");
		Files.createDirectory(path);

		Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwxr-x---");

		pathFileSystem.setPosixFilePermissions(path, permissions);

		assertEquals(Files.getPosixFilePermissions(path), permissions);
	}

	private void makeSureBasePathExistsAndIsEmpty() throws IOException {
		Path basePath = Paths.get(FILE_SYSTEM_BASE_PATH);
		Files.createDirectories(basePath);
		deleteFiles(basePath);
	}

	private void deleteFiles(Path path) throws IOException {
		try (Stream<Path> paths = Files.list(path)) {
			paths.forEach(this::deletePathQuietly);
		}
	}

	private void deletePathQuietly(Path path) {
		try {
			if (Files.isDirectory(path)) {
				deleteFiles(path);
			}
			Files.deleteIfExists(path);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void removeTempFiles() throws IOException {
		Path basePath = Paths.get(FILE_SYSTEM_BASE_PATH);
		if (Files.exists(basePath)) {
			deleteFiles(basePath);
			Files.deleteIfExists(basePath);
		}
	}
}