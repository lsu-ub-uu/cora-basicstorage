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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import se.uu.ub.cora.storage.StorageException;
import se.uu.ub.cora.storage.StreamPathBuilder;

public class StreamPathBuilderImp implements StreamPathBuilder {

	private static final String STREAMS_DIR = "streams";
	private static final String CAN_NOT_WRITE_FILES_TO_DISK = "can not write files to disk: ";
	private String fileSystemBasePath;

	public StreamPathBuilderImp(String fileSystemBasePath) {
		this.fileSystemBasePath = fileSystemBasePath;
	}

	@Override
	public String buildPathToAFileAndEnsureFolderExists(String dataDivider, String type,
			String id) {
		Path pathByDataDivider = Paths.get(fileSystemBasePath, STREAMS_DIR, dataDivider);
		ensureStorageDirectoryExists(pathByDataDivider);
		return buildFileStoragePathToAFile(pathByDataDivider, type, id);
	}

	private void ensureStorageDirectoryExists(Path pathByDataDivider) {
		if (storageDirectoryDoesNotExist(pathByDataDivider)) {
			tryToCreateStorageDirectory(pathByDataDivider);
		}
	}

	private boolean storageDirectoryDoesNotExist(Path pathByDataDivider) {
		return !Files.exists(pathByDataDivider);
	}

	private void tryToCreateStorageDirectory(Path pathByDataDivider) {
		try {
			String permissions = "rwxrwxrwx";
			Files.createDirectories(pathByDataDivider);
			Files.setPosixFilePermissions(pathByDataDivider, createFilePermissions(permissions));
			Files.setPosixFilePermissions(Paths.get(fileSystemBasePath, STREAMS_DIR),
					createFilePermissions(permissions));
		} catch (IOException e) {
			throw StorageException.withMessageAndException(CAN_NOT_WRITE_FILES_TO_DISK + e, e);
		}
	}

	private Set<PosixFilePermission> createFilePermissions(String permissions) {
		return PosixFilePermissions.fromString(permissions);
	}

	private String buildFileStoragePathToAFile(Path path, String type, String id) {
		return path.toString() + "/" + type + ":" + id;
	}

	public String onlyForTestGetFileSystemBasePath() {
		return fileSystemBasePath;
	}
}
