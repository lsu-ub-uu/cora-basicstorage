/*
 * Copyright 2016 Uppsala University Library
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

import se.uu.ub.cora.storage.ResourceNotFoundException;
import se.uu.ub.cora.storage.StorageException;
import se.uu.ub.cora.storage.StreamStorage;

public class StreamStorageOnDiskTest {
	private static final String DATA_DIVIDER = "someDataDivider";
	private static final String STREAM_ID = "someStreamId";
	private String basePath = "/tmp/streamStorageOnDiskTempStream/";
	private StreamStorage streamStorage;
	private InputStream streamToStore;

	@BeforeMethod
	public void setUpForTests() throws IOException {
		makeSureBasePathExistsAndIsEmpty();
		streamStorage = StreamStorageOnDisk.usingBasePath(basePath);
		streamToStore = createTestInputStreamToStore();
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
			StreamStorageOnDisk.usingBasePath("/root/streamsDOESNOTEXIST");
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
		StreamStorageOnDisk.usingBasePath(basePath);
	}

	@Test
	public void testGetBasePath() throws IOException {
		StreamStorageOnDisk streamStorageOnDisk = (StreamStorageOnDisk) streamStorage;
		assertEquals(streamStorageOnDisk.getBasePath(), basePath);
	}

	@Test
	public void storeCreatePathForNewDataDivider() {
		streamStorage.store(STREAM_ID, DATA_DIVIDER, streamToStore);

		Path pathDataDivider = Paths.get(basePath, DATA_DIVIDER);
		assertTrue(Files.exists(pathDataDivider));
	}

	private InputStream createTestInputStreamToStore() {
		InputStream stream = new ByteArrayInputStream("a string".getBytes(StandardCharsets.UTF_8));
		return stream;
	}

	@Test
	public void storeFileForStreamPathIsEmptySentAlongException() throws IOException {
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
	public void storeFileForStream() {
		long size = streamStorage.store(STREAM_ID, DATA_DIVIDER, streamToStore);

		assertTrue(Files.exists(Paths.get(basePath, DATA_DIVIDER, STREAM_ID)));
		assertEquals(String.valueOf(size), "8");
	}

	@Test
	public void storeFileForStreamDirectoriesAlreadyExist() {
		long size = streamStorage.store(STREAM_ID, DATA_DIVIDER, streamToStore);

		assertTrue(Files.exists(Paths.get(basePath, DATA_DIVIDER, STREAM_ID)));
		assertEquals(String.valueOf(size), "8");

		InputStream stream2 = createTestInputStreamToStore();

		long size2 = streamStorage.store("someStreamId2", DATA_DIVIDER, stream2);

		assertTrue(Files.exists(Paths.get(basePath, DATA_DIVIDER, "someStreamId2")));
		assertEquals(String.valueOf(size2), "8");
	}

	@Test
	public void retreive() throws IOException {
		storeStream();

		InputStream stream = streamStorage.retrieve(STREAM_ID, DATA_DIVIDER);
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
	public void retreiveFolderForDataDividerIsMissing() {
		try {
			streamStorage.retrieve(STREAM_ID, DATA_DIVIDER);
			fail("Should have thrown an exception");
		} catch (Exception e) {
			assertTrue(e instanceof ResourceNotFoundException);
			assertEquals(e.getMessage(),
					"Could not read stream from disk, no such folder: someDataDivider");
		}
	}

	@Test()
	public void retreiveStreamIsMissing() {
		storeStream();
		try {
			streamStorage.retrieve("someStreamIdDOESNOTEXIST", DATA_DIVIDER);
			fail("Should have thrown an exception");
		} catch (Exception e) {
			assertTrue(e instanceof ResourceNotFoundException);
			assertEquals(e.getMessage(),
					"Could not read stream from disk, no such streamId: someStreamIdDOESNOTEXIST");
		}
	}

	private long storeStream() {
		StreamStorageOnDisk creatingStuffOnDiskStreamStorage = StreamStorageOnDisk
				.usingBasePath(basePath);
		return creatingStuffOnDiskStreamStorage.store(STREAM_ID, DATA_DIVIDER, streamToStore);
	}

	@Test
	public void retreivePathForStreamIsBrokenSentAlongException() throws IOException {
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
	public void delete() throws Exception {
		storeStream();

		streamStorage.delete(STREAM_ID, DATA_DIVIDER);

		assertNotFound();

	}

	@Test
	public void delete_RemoveFolderIfEmpty() throws Exception {
		storeStream();

		streamStorage.delete(STREAM_ID, DATA_DIVIDER);

		assertFalse(Files.exists(Paths.get(basePath, DATA_DIVIDER)));
	}

	@Test
	public void deleteStream_FolderMissing() throws Exception {
		try {
			streamStorage.delete(STREAM_ID, DATA_DIVIDER);
			fail("Should have thrown an exception");
		} catch (Exception e) {
			assertTrue(e instanceof ResourceNotFoundException);
			assertEquals(e.getMessage(),
					"Could not delete stream from disk, no such folder: someDataDivider");
		}
	}

	@Test()
	public void deleteStream_StreamMissing() {
		storeStream();
		try {
			streamStorage.delete("someStreamIdDOESNOTEXIST", DATA_DIVIDER);
			fail("Should have thrown an exception");
		} catch (Exception e) {
			assertTrue(e instanceof ResourceNotFoundException);
			assertEquals(e.getMessage(),
					"Could not delete stream from disk, no such streamId: someStreamIdDOESNOTEXIST");
		}
	}

	@Test
	public void deleteStream_ExceptionStreamFromDisk() throws Exception {
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
	public void deleteStream_ExceptionFolderFromDisk() throws Exception {
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
			streamStorage.retrieve(STREAM_ID, DATA_DIVIDER);
			fail("It should throw exception");
		} catch (Exception e) {
			assertTrue(e instanceof ResourceNotFoundException);
		}
	}
}
