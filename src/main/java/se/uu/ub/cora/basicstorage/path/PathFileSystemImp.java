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

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

public class PathFileSystemImp implements PathFileSystem {

	public static PathFileSystemImp usingRealFileSystem() {
		return new PathFileSystemImp();
	}

	private PathFileSystemImp() {
		// no-op
	}

	@Override
	public boolean notExists(Path path) {
		return Files.notExists(path);
	}

	@Override
	public void createDirectory(Path path) {
		try {
			Files.createDirectory(path);
		} catch (FileAlreadyExistsException _) {
			// Do nothing, continue, folder already exists
		} catch (Exception e) {
			throw PathFileSystemException
					.withMessageAndException("Could not create directory " + path.toString(), e);
		}
	}

	@Override
	public void setPosixFilePermissions(Path path, Set<PosixFilePermission> permissions) {
		try {
			Files.setPosixFilePermissions(path, permissions);
		} catch (Exception e) {
			throw PathFileSystemException
					.withMessageAndException("Could not create directory " + path.toString(), e);
		}
	}
}