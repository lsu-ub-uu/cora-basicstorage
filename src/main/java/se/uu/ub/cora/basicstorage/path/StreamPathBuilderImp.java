/*
 * Copyright 2023, 2025 Uppsala University Library
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
import java.text.MessageFormat;
import java.util.Set;

import se.uu.ub.cora.storage.StorageException;
import se.uu.ub.cora.storage.StreamPathBuilder;
import se.uu.ub.cora.storage.hash.CoraDigestor;

public class StreamPathBuilderImp implements StreamPathBuilder {
	private static final String STREAMS_DIR = "streams";

	public static StreamPathBuilderImp usingBasePathAndCoraDigestor(String fileSystemBasePath,
			CoraDigestor digestor) {
		return new StreamPathBuilderImp(fileSystemBasePath, digestor);
	}

	private String fileSystemBasePath;
	private CoraDigestor digestor;

	private StreamPathBuilderImp(String fileSystemBasePath, CoraDigestor digestor) {
		this.fileSystemBasePath = fileSystemBasePath;
		this.digestor = digestor;
	}

	@Override
	public String buildPathToAFile(String dataDivider, String type, String id,
			String representation) {
		Path pathToFolder = createPathToFolder(dataDivider, type, id);
		return buildFileStoragePathToAFileAndRepresentation(pathToFolder, type, id, representation);
	}

	@Override
	public String buildPathToAFileAndEnsureFolderExists(String dataDivider, String type, String id,
			String representation) {
		ensureBasePathExistsOtherwiseThrowException();
		Path pathToFolder = createFoldersAndEnsureTheyExists(dataDivider, type, id);
		return buildFileStoragePathToAFileAndRepresentation(pathToFolder, type, id, representation);
	}

	private Path createFoldersAndEnsureTheyExists(String dataDivider, String type, String id) {
		Path pathToFolder = createPathToFolder(dataDivider, type, id);
		ensureStorageDirectoryExists(pathToFolder);
		return pathToFolder;
	}

	private void ensureBasePathExistsOtherwiseThrowException() {
		Path basePath = Paths.get(fileSystemBasePath);
		if (storageDirectoryDoesNotExist(basePath)) {
			throw StorageException
					.withMessage("File base path " + basePath.toString() + " does not exists.");
		}
	}

	private Path createPathToFolder(String dataDivider, String type, String id) {
		String typeAndIdAsSha256 = digestor.stringToSha256Hex(type + ":" + id);
		return buildPathToFolderUsingSha256Path(typeAndIdAsSha256, dataDivider);
	}

	private Path buildPathToFolderUsingSha256Path(String sha256hex, String dataDivider) {
		String sha256HexLowerCase = sha256hex.toLowerCase();
		String folder1 = sha256HexLowerCase.substring(0, 3);
		String folder2 = sha256HexLowerCase.substring(3, 6);
		String folder3 = sha256HexLowerCase.substring(6, 9);
		String folder4 = sha256HexLowerCase;

		return Paths.get(fileSystemBasePath, STREAMS_DIR, dataDivider, folder1, folder2, folder3,
				folder4);
	}

	private void ensureStorageDirectoryExists(Path pathToFolder) {
		if (storageDirectoryDoesNotExist(pathToFolder)) {
			tryToCreateStorageDirectory(pathToFolder);
		}
	}

	private boolean storageDirectoryDoesNotExist(Path pathByDataDivider) {
		return !Files.exists(pathByDataDivider);
	}

	private void tryToCreateStorageDirectory(Path pathToFolder) {
		try {
			Path basePath = Paths.get(fileSystemBasePath);
			createPublicFolders(basePath, pathToFolder, "rwxrwxrwx");
		} catch (Exception e) {
			String pathPattern = "Failed to create folder {0}, in filesystem";
			String message = MessageFormat.format(pathPattern, pathToFolder);
			throw StorageException.withMessageAndException(message, e);
		}
	}

	private void createPublicFolders(Path basePath, Path pathToFolder, String permissions)
			throws IOException {
		Set<PosixFilePermission> perms = createFilePermissions(permissions);
		Path currentPath = basePath;
		for (Path folderNameAsPath : basePath.relativize(pathToFolder)) {
			currentPath = currentPath.resolve(folderNameAsPath);
			createFolderIfNotExisting(currentPath, perms);
		}
	}

	private void createFolderIfNotExisting(Path current, Set<PosixFilePermission> perms)
			throws IOException {
		if (Files.notExists(current)) {
			Files.createDirectory(current);
		}
		Files.setPosixFilePermissions(current, perms);
	}

	private Set<PosixFilePermission> createFilePermissions(String permissions) {
		return PosixFilePermissions.fromString(permissions);
	}

	private String buildFileStoragePathToAFileAndRepresentation(Path path, String type, String id,
			String representation) {
		String pathPattern = "{0}/{1}:{2}-{3}";
		return MessageFormat.format(pathPattern, path.toString(), type, id, representation);
	}

	public String onlyForTestGetFileSystemBasePath() {
		return fileSystemBasePath;
	}

	public CoraDigestor onlyForTestGetCoraDigestor() {
		return digestor;
	}

}
