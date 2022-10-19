module se.uu.ub.cora.basicstorage {
	requires transitive se.uu.ub.cora.gatekeeper;
	requires se.uu.ub.cora.json;
	requires se.uu.ub.cora.logger;
	requires transitive se.uu.ub.cora.storage;
	requires se.uu.ub.cora.basicdata;
	requires se.uu.ub.cora.initialize;

	exports se.uu.ub.cora.basicstorage;

	provides se.uu.ub.cora.storage.RecordStorageInstanceProvider
			with se.uu.ub.cora.basicstorage.RecordStorageOnDiskProvider;

	provides se.uu.ub.cora.storage.StreamStorageProvider
			with se.uu.ub.cora.basicstorage.StreamStorageOnDiskProvider;
	provides se.uu.ub.cora.storage.RecordIdGeneratorProvider
			with se.uu.ub.cora.basicstorage.TimeStampIdGeneratorProvider;
}