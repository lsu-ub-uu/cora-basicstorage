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

import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

/**
 * PathFileSystem abstracts filesystem operations on {@link Path} objects used by
 * {@link StreamPathBuilderImp}.
 * <p>
 * The interface exists to isolate calls to the underlying filesystem and make code that creates
 * directories and sets permissions testable without depending directly on static methods in
 * {@code java.nio.file.Files}.
 * <p>
 * Implementations may delegate to the real filesystem or provide test doubles such as spies or
 * fakes.
 */
public interface PathFileSystem {

	/**
	 * Checks if the supplied path does not exist in the filesystem.
	 *
	 * @param path
	 *            the path to check
	 * @return {@code true} if the path does not exist, otherwise {@code false}
	 */
	boolean notExists(Path path);

	/**
	 * Creates a directory at the supplied path.
	 *
	 * @param path
	 *            the path for the directory to create
	 * @throws PathFileSystemException
	 *             if the directory could not be created
	 */
	void createDirectory(Path path);

	/**
	 * Sets POSIX permissions for the supplied path.
	 *
	 * @param path
	 *            the path whose permissions should be changed
	 * @param permissions
	 *            the permissions to apply
	 * @throws PathFileSystemException
	 *             if the permissions could not be set
	 */
	void setPosixFilePermissions(Path path, Set<PosixFilePermission> permissions);
}
