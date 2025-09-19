/*
 * Copyright 2016, 2025 Uppsala University Library
 * Copyright 2016 Olov McKie
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
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.basicstorage.path.StreamPathBuilderImp;
import se.uu.ub.cora.storage.ResourceNotFoundException;
import se.uu.ub.cora.storage.StorageException;
import se.uu.ub.cora.storage.StreamPathBuilder;
import se.uu.ub.cora.storage.StreamStorage;
import se.uu.ub.cora.storage.spies.hash.CoraDigestorSpy;

public class StreamStorageOnDiskTest {
	private static final String DATA_DIVIDER = "someDataDivider";
	private static final String TYPE = "someType";
	private static final String ID = "someId";
	private static final String REPRESENTATION = "someRepresentation";
	private String basePath = "/tmp/streamStorageOnDiskTempStream/";
	private StreamStorage streamStorage;
	private InputStream streamToStore;
	private StreamPathBuilder streamPathBuilder;
	private CoraDigestorSpy digestor;

	@BeforeMethod
	public void setUpForTests() throws IOException {
		makeSureBasePathExistsAndIsEmpty();

		setUpStreamPathBuilder();
		streamStorage = StreamStorageOnDisk.usingBasePathAndStreamPathBuilder(basePath,
				streamPathBuilder);
		streamToStore = createTestInputStreamToStore("a string");
	}

	private void setUpStreamPathBuilder() {
		digestor = new CoraDigestorSpy();
		digestor.MRV.setDefaultReturnValuesSupplier("stringToSha256Hex", () -> "123456789abcdef");
		streamPathBuilder = StreamPathBuilderImp.usingBasePathAndCoraDigestor(basePath, digestor);
	}

	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(basePath);
		dir.mkdir();
		deleteFiles(basePath);
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
		if (Files.exists(Paths.get(basePath))) {
			deleteFiles(basePath);
			File dir = new File(basePath);
			dir.delete();
		}
	}

	@Test
	public void testInitNoPermissionOnPathSentAlongException() throws IOException {
		try {
			removeTempFiles();
			StreamStorageOnDisk.usingBasePathAndStreamPathBuilder("/root/streamsDOESNOTEXIST",
					streamPathBuilder);
			fail("Should have thrown an exception");
		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertTrue(e.getCause() instanceof AccessDeniedException);
			assertEquals(e.getMessage(), "Could not write files to disk: "
					+ "java.nio.file.AccessDeniedException: /root/streamsDOESNOTEXIST");
		}
	}

	@Test
	public void testInitMissingPath() throws IOException {
		removeTempFiles();
		StreamStorageOnDisk.usingBasePathAndStreamPathBuilder(basePath, streamPathBuilder);
	}

	@Test
	public void storeCreatePathForNewDataDivider() {
		long streamSize = streamStorage.store(DATA_DIVIDER, TYPE, ID, REPRESENTATION,
				streamToStore);

		Path pathDataDivider = Paths.get(basePath, "streams", DATA_DIVIDER, "123", "456", "789",
				"123456789abcdef", TYPE + ":" + ID + "-" + REPRESENTATION);
		assertTrue(Files.exists(pathDataDivider));
		assertEquals(String.valueOf(streamSize), "8");
	}

	private InputStream createTestInputStreamToStore(String value) {
		return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
	}

	@Test
	public void storeFileForStreamPathIsEmptySentAlongException() {
		try {
			((StreamStorageOnDisk) streamStorage).tryToStoreStream(streamToStore, Paths.get(""));
			fail("Should have thrown an exception");
		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertTrue(e.getCause() instanceof FileSystemException);
			assertEquals(e.getMessage(),
					"Could not write files to disk: java.nio.file.FileSystemException: : Is a directory");
		}
	}

	@Test
	public void storeFileForStreamDirectoriesAlreadyExist_streamReplaced() {
		long streamSize = streamStorage.store(DATA_DIVIDER, TYPE, ID, REPRESENTATION,
				streamToStore);

		assertEquals(String.valueOf(streamSize), "8");

		InputStream stream2 = createTestInputStreamToStore("another string");
		long streamSize2 = streamStorage.store(DATA_DIVIDER, TYPE, ID, REPRESENTATION, stream2);

		assertEquals(String.valueOf(streamSize2), "14");

		Path pathToFile = Paths.get(basePath, "streams", DATA_DIVIDER, "123", "456", "789",
				"123456789abcdef", TYPE + ":" + ID + "-" + REPRESENTATION);
		assertTrue(Files.exists(pathToFile));
	}

	@Test
	public void retreive() throws IOException {
		storeStream();

		InputStream stream = streamStorage.retrieve(DATA_DIVIDER, TYPE, ID, REPRESENTATION);
		assertNotNull(stream);

		ByteArrayOutputStream result = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int length;
		while ((length = stream.read(buffer)) != -1) {
			result.write(buffer, 0, length);
		}
		String stringFromStream = result.toString("UTF-8");

		assertEquals(stringFromStream, "a string");
	}

	@Test()
	public void retreiveStreamIsMissing() {
		try {
			streamStorage.retrieve(DATA_DIVIDER, TYPE, "someStreamIdDOESNOTEXIST", REPRESENTATION);
			fail("Should have thrown an exception");
		} catch (Exception e) {
			assertTrue(e instanceof ResourceNotFoundException);
			assertEquals(e.getMessage(), "Could not read stream from disk, no such stream: " + TYPE
					+ ":someStreamIdDOESNOTEXIST-" + REPRESENTATION);

			assertBilPathToFileCalled();
		}
	}

	private void assertBilPathToFileCalled() {
		assertFalse(Files.exists(Paths.get(basePath, "streams", DATA_DIVIDER, "123", "456", "789",
				"123456789abcdef")));
	}

	private long storeStream() {
		StreamStorageOnDisk creatingStuffOnDiskStreamStorage = StreamStorageOnDisk
				.usingBasePathAndStreamPathBuilder(basePath, streamPathBuilder);
		return creatingStuffOnDiskStreamStorage.store(DATA_DIVIDER, TYPE, ID, REPRESENTATION,
				streamToStore);
	}

	@Test
	public void retreivePathForStreamIsBrokenSentAlongException() {
		try {
			((StreamStorageOnDisk) streamStorage).tryToReadStream(Paths.get("/broken/path"));
			fail("Should have thrown an exception");
		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertTrue(e.getCause() instanceof FileSystemException);
			assertEquals(e.getMessage(),
					"Could not read stream from disk: java.nio.file.NoSuchFileException: /broken/path");
		}
	}

	@Test
	public void delete() {
		storeStream();

		streamStorage.delete(DATA_DIVIDER, TYPE, ID, REPRESENTATION);

		assertNotFound();
	}

	@Test
	public void delete_RemoveFolderIfEmpty() {
		storeStream();

		streamStorage.delete(DATA_DIVIDER, TYPE, ID, REPRESENTATION);

		assertFalse(Files.exists(Paths.get(basePath, "streams")));
		assertTrue(Files.exists(Paths.get(basePath)));
	}

	@Test
	public void delete_LeaveFolderNotEmpty() {
		storeStream();
		StreamStorageOnDisk creatingStuffOnDiskStreamStorage = StreamStorageOnDisk
				.usingBasePathAndStreamPathBuilder(basePath, streamPathBuilder);
		creatingStuffOnDiskStreamStorage.store(DATA_DIVIDER, TYPE, ID, "otherRepresentation",
				streamToStore);

		streamStorage.delete(DATA_DIVIDER, TYPE, ID, REPRESENTATION);

		assertFalse(Files.exists(Paths.get(basePath, "streams", DATA_DIVIDER, "123", "456", "789",
				"123456789abcdef", TYPE + ":" + ID + "-" + REPRESENTATION)));
		assertTrue(Files.exists(Paths.get(basePath, "streams", DATA_DIVIDER, "123", "456", "789",
				"123456789abcdef", TYPE + ":" + ID + "-" + "otherRepresentation")));
		assertTrue(Files.exists(Paths.get(basePath, "streams")));
		assertTrue(Files.exists(Paths.get(basePath)));
	}

	@Test()
	public void deleteStream_StreamMissing() {
		try {
			streamStorage.delete(DATA_DIVIDER, TYPE, "someStreamIdDOESNOTEXIST", REPRESENTATION);
			fail("Should have thrown an exception");
		} catch (Exception e) {
			assertTrue(e instanceof ResourceNotFoundException);
			assertEquals(e.getMessage(), "Could not delete stream from disk, no such stream: "
					+ TYPE + ":someStreamIdDOESNOTEXIST-" + REPRESENTATION);
		}
	}

	@Test
	public void deleteStream_ExceptionStreamFromDisk() {
		try {
			((StreamStorageOnDisk) streamStorage).tryToDeleteStream(Paths.get("/somePath"));
			fail("Should have thrown an exception");
		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertTrue(e.getCause() instanceof FileSystemException);
			assertEquals(e.getMessage(),
					"Could not delete stream from disk: java.nio.file.NoSuchFileException: /somePath");
		}
	}

	@Test
	public void deleteStream_ExceptionFolderFromDisk() {
		try {
			((StreamStorageOnDisk) streamStorage).deleteFolderIfEmpty(Paths.get("/somePath"));
			fail("Should have thrown an exception");
		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertTrue(e.getCause() instanceof FileSystemException);
			assertEquals(e.getMessage(),
					"Could not delete folder from disk: java.nio.file.NoSuchFileException: /somePath");
		}
	}

	private void assertNotFound() {
		try {
			streamStorage.retrieve(DATA_DIVIDER, TYPE, ID, REPRESENTATION);
			fail("It should throw exception");
		} catch (Exception e) {
			assertTrue(e instanceof ResourceNotFoundException);
		}
	}

	@Test
	public void testOnlyForTestGetBasePath() {
		StreamStorageOnDisk streamStorageOnDisk = (StreamStorageOnDisk) streamStorage;
		assertEquals(streamStorageOnDisk.onlyForTestGetBasePath(), basePath);
	}

	@Test
	public void testOnlyForTestGetStreamPathBuilder() {
		StreamPathBuilder strem = ((StreamStorageOnDisk) streamStorage)
				.onlyForTestGetStreamPathBuilder();
		assertSame(strem, streamPathBuilder);
	}
}
