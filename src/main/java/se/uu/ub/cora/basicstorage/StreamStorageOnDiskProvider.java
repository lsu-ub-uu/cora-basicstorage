/*
 * Copyright 2019, 2025 Uppsala University Library
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

import java.util.Map;

import se.uu.ub.cora.basicstorage.path.StreamPathBuilderImp;
import se.uu.ub.cora.logger.Logger;
import se.uu.ub.cora.logger.LoggerProvider;
import se.uu.ub.cora.storage.StreamStorage;
import se.uu.ub.cora.storage.StreamStorageProvider;
import se.uu.ub.cora.storage.hash.imp.CoraDigestorImp;

public class StreamStorageOnDiskProvider implements StreamStorageProvider {
	private Logger log = LoggerProvider.getLoggerForClass(StreamStorageOnDiskProvider.class);
	private Map<String, String> initInfo;
	private StreamStorageOnDisk streamStorage;

	@Override
	public int getOrderToSelectImplementionsBy() {
		return 0;
	}

	@Override
	public void startUsingInitInfo(Map<String, String> initInfo) {
		this.initInfo = initInfo;
		log.logInfoUsingMessage("StreamStorageOnDiskProvider starting StreamStorageOnDisk...");
		startStreamStorage();
		log.logInfoUsingMessage("StreamStorageOnDiskProvider started StreamStorageOnDisk");
	}

	private void startStreamStorage() {
		String basePath = tryToGetInitParameter("storageOnDiskBasePath");
		var streamPathBuilder = StreamPathBuilderImp.usingBasePathAndCoraDigestor(basePath,
				new CoraDigestorImp());
		streamStorage = StreamStorageOnDisk.usingBasePathAndStreamPathBuilder(basePath,
				streamPathBuilder);
	}

	private String tryToGetInitParameter(String parameterName) {
		throwErrorIfKeyIsMissingFromInitInfo(parameterName);
		String parameter = initInfo.get(parameterName);
		log.logInfoUsingMessage("Found " + parameter + " as " + parameterName);
		return parameter;
	}

	private void throwErrorIfKeyIsMissingFromInitInfo(String key) {
		if (!initInfo.containsKey(key)) {
			String errorMessage = "InitInfo must contain " + key;
			log.logFatalUsingMessage(errorMessage);
			throw DataStorageException.withMessage(errorMessage);
		}
	}

	@Override
	public StreamStorage getStreamStorage() {
		return streamStorage;
	}

}
