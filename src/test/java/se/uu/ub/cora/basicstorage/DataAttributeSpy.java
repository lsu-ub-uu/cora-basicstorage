package se.uu.ub.cora.basicstorage;

import se.uu.ub.cora.data.DataAttribute;

public class DataAttributeSpy implements DataAttribute {

	private String id;
	private String value;

	public DataAttributeSpy(String id, String value) {
		this.id = id;
		this.value = value;
	}

	@Override
	public String getNameInData() {
		return id;
	}

	@Override
	public String getValue() {
		return value;
	}

}
