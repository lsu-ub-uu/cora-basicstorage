module se.uu.ub.cora.basicstorage {
	requires se.uu.ub.cora.json;
	requires transitive se.uu.ub.cora.storage;
	requires se.uu.ub.cora.initialize;
	requires se.uu.ub.cora.logger;

	exports se.uu.ub.cora.basicstorage;
	exports se.uu.ub.cora.basicstorage.path;

	provides se.uu.ub.cora.storage.StreamStorageProvider
			with se.uu.ub.cora.basicstorage.StreamStorageOnDiskProvider;
}