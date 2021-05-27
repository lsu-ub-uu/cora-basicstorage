/*
 * Copyright 2016 Uppsala University Library
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

import se.uu.ub.cora.storage.MetadataStorage;
import se.uu.ub.cora.storage.RecordStorage;

public final class RecordStorageInMemoryReadFromDisk extends RecordStorageOnDisk
		implements RecordStorage, MetadataStorage {

	public static RecordStorageInMemoryReadFromDisk createRecordStorageOnDiskWithBasePath(
			String basePath) {
		return new RecordStorageInMemoryReadFromDisk(basePath);
	}

	private RecordStorageInMemoryReadFromDisk(String basePath) {
		super(basePath);
	}

	@Override
	protected void writeDataToDisk(String recordType, String dataDivider) {
		// do not write to disk
	}

	@Override
	public void deleteByTypeAndId(String recordType, String recordId) {
		// TODO Auto-generated method stub
		
	}
}
