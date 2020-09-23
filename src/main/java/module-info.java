module se.uu.ub.cora.basicstorage {
	requires transitive se.uu.ub.cora.gatekeeper;
	requires se.uu.ub.cora.json;
	requires transitive se.uu.ub.cora.apptokenstorage;
	requires se.uu.ub.cora.logger;
	requires transitive se.uu.ub.cora.storage;
	requires se.uu.ub.cora.searchstorage;

	exports se.uu.ub.cora.basicstorage;

	provides se.uu.ub.cora.gatekeeper.user.UserStorageProvider
			with se.uu.ub.cora.basicstorage.OnDiskUserStorageProvider;
	provides se.uu.ub.cora.gatekeeper.user.GuestUserStorageProvider
			with se.uu.ub.cora.basicstorage.OnDiskGuestUserStorageProvider;
	provides se.uu.ub.cora.apptokenstorage.AppTokenStorageProvider
			with se.uu.ub.cora.basicstorage.OnDiskAppTokenStorageProvider;

	provides se.uu.ub.cora.storage.RecordStorageProvider
			with se.uu.ub.cora.basicstorage.RecordStorageOnDiskProvider;
	provides se.uu.ub.cora.storage.MetadataStorageProvider
			with se.uu.ub.cora.basicstorage.RecordStorageOnDiskProvider;
	provides se.uu.ub.cora.storage.StreamStorageProvider
			with se.uu.ub.cora.basicstorage.StreamStorageOnDiskProvider;

	provides se.uu.ub.cora.storage.RecordIdGeneratorProvider
			with se.uu.ub.cora.basicstorage.TimeStampIdGeneratorProvider;

}