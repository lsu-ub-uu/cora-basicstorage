package se.uu.ub.cora.basicstorage.path;

import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;
import se.uu.ub.cora.testutils.mrv.MethodReturnValues;

public class PathFileSystemSpy implements PathFileSystem {
	public MethodCallRecorder MCR = new MethodCallRecorder();
	public MethodReturnValues MRV = new MethodReturnValues();

	public PathFileSystemSpy() {
		MCR.useMRV(MRV);
		MRV.setDefaultReturnValuesSupplier("notExists", () -> false);
	}

	@Override
	public boolean notExists(Path path) {
		return (boolean) MCR.addCallAndReturnFromMRV("path", path);
	}

	@Override
	public void createDirectory(Path path) {
		MCR.addCall("path", path);
	}

	@Override
	public void setPosixFilePermissions(Path path, Set<PosixFilePermission> permissions) {
		MCR.addCall("path", path, "permissions", permissions);
	}
}