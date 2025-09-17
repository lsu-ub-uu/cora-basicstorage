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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;

import se.uu.ub.cora.storage.ResourceNotFoundException;
import se.uu.ub.cora.storage.StorageException;
import se.uu.ub.cora.storage.StreamPathBuilder;
import se.uu.ub.cora.storage.StreamStorage;

public final class StreamStorageOnDisk implements StreamStorage {

	private static final String CAN_NOT_WRITE_FILES_TO_DISK = "Could not write files to disk: ";
	private static final int BUFFER_LENGTH = 1024;
	private String basePath;
	private StreamPathBuilder streamPathBuilder;

	private StreamStorageOnDisk(String basePath, StreamPathBuilder streamPathBuilder) {
		this.basePath = basePath;
		this.streamPathBuilder = streamPathBuilder;
		if (basePathDoesNotExist(basePath)) {
			createBaseDirectory(basePath);
		}
	}

	public static StreamStorageOnDisk usingBasePathAndStreamPathBuilder(String basePath,
			StreamPathBuilder streamPathBuilder) {
		return new StreamStorageOnDisk(basePath, streamPathBuilder);
	}

	private boolean basePathDoesNotExist(String basePath) {
		return !Files.exists(Paths.get(basePath));
	}

	private void createBaseDirectory(String basePath) {
		tryToCreateStorageDirectory(Paths.get(basePath));
	}

	@Override
	public long store(String dataDivider, String type, String id, String representation,
			InputStream stream) {
		String pathToFile = streamPathBuilder.buildPathToAFileAndEnsureFolderExists(dataDivider,
				type, id, representation);

		return tryToStoreStream(stream, Paths.get(pathToFile));
	}

	long tryToStoreStream(InputStream stream, Path path) {
		try {
			return storeStream(stream, path);
		} catch (IOException e) {
			throw StorageException.withMessageAndException(CAN_NOT_WRITE_FILES_TO_DISK + e, e);
		}
	}

	private long storeStream(InputStream stream, Path path) throws IOException {
		OutputStream outputStream = Files.newOutputStream(path);
		long size = storeStreamUsingOutputStream(stream, outputStream);
		outputStream.flush();
		outputStream.close();
		return size;
	}

	private long storeStreamUsingOutputStream(InputStream stream, OutputStream outputStream)
			throws IOException {
		long size = 0;
		byte[] bytes = new byte[BUFFER_LENGTH];

		int written;
		while ((written = stream.read(bytes)) != -1) {
			outputStream.write(bytes, 0, written);
			size += written;
		}
		return size;
	}

	private boolean storagePathDoesNotExist(Path pathByDataDivider) {
		return !Files.exists(pathByDataDivider);
	}

	private void tryToCreateStorageDirectory(Path pathByDataDivider) {
		try {
			Files.createDirectory(pathByDataDivider);
		} catch (IOException e) {
			throw StorageException.withMessageAndException(CAN_NOT_WRITE_FILES_TO_DISK + e, e);
		}
	}

	@Override
	public InputStream retrieve(String dataDivider, String type, String id, String representation) {
		String pathToFile = streamPathBuilder.buildPathToAFile(dataDivider, type, id,
				representation);
		Path path = Paths.get(pathToFile);
		if (storagePathDoesNotExist(path)) {
			String streamName = getStreamName(type, id, representation);
			throw ResourceNotFoundException
					.withMessage("Could not read stream from disk, no such stream: " + streamName);
		}
		return tryToReadStream(path);
	}

	private String getStreamName(String type, String id, String representation) {
		String fileFormat = "{0}:{1}-{2}";
		MessageFormat.format(fileFormat, type, id);
		return MessageFormat.format(fileFormat, type, id, representation);
	}

	InputStream tryToReadStream(Path path) {
		try {
			return readStream(path);
		} catch (IOException e) {
			throw StorageException.withMessageAndException("Could not read stream from disk: " + e,
					e);
		}
	}

	InputStream readStream(Path path) throws IOException {
		return Files.newInputStream(path);

	}

	@Override
	public void delete(String dataDivider, String type, String id, String representation) {
		String pathToFile = streamPathBuilder.buildPathToAFile(dataDivider, type, id,
				representation);
		Path path = Paths.get(pathToFile);
		if (storagePathDoesNotExist(path)) {
			throw ResourceNotFoundException
					.withMessage("Could not delete stream from disk, no such stream: "
							+ getStreamName(type, id, representation));
		}
		tryToDeleteStream(path);
	}

	void deleteFolderIfEmpty(Path pathByDataDivider) {
		try (DirectoryStream<Path> entries = Files.newDirectoryStream(pathByDataDivider)) {
			if (!entries.iterator().hasNext()) {
				Files.delete(pathByDataDivider);
			}
		} catch (Exception e) {
			throw StorageException
					.withMessageAndException("Could not delete folder from disk: " + e, e);
		}
	}

	void tryToDeleteStream(Path path) {
		try {
			Files.delete(path);
			removeEmptyFoldersToBase(path.getParent(), Paths.get(basePath));
		} catch (Exception e) {
			throw StorageException
					.withMessageAndException("Could not delete stream from disk: " + e, e);
		}
	}

	private void removeEmptyFoldersToBase(Path startPath, Path basePath) throws IOException {
		Path current = startPath;
		while (stillBelowBasePathAndEmptyDirectory(basePath, current)) {
			Files.delete(current);
			current = current.getParent();
		}
	}

	private boolean stillBelowBasePathAndEmptyDirectory(Path basePath, Path current)
			throws IOException {
		return stillBelowBasePath(basePath, current) && directoryIsEmpty(current);
	}

	private boolean stillBelowBasePath(Path basePath, Path current) {
		return !current.equals(basePath);
	}

	private boolean directoryIsEmpty(Path current) throws IOException {
		try (DirectoryStream<Path> entries = Files.newDirectoryStream(current)) {
			if (entries.iterator().hasNext()) {
				return false;
			}
		}
		return true;
	}

	public String onlyForTestGetBasePath() {
		return basePath;
	}

	public StreamPathBuilder onlyForTestGetStreamPathBuilder() {
		return streamPathBuilder;
	}

}
