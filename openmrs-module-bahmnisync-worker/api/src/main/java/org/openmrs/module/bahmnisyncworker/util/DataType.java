package org.openmrs.module.bahmnisyncworker.util;

public enum DataType {
	INT32("int32"), INT64("int64"), INT16("int16"), STRING("string"), DOUBLE("double");
	
	public final String label;
	
	private DataType(String label) {
		this.label = label;
	}
}
