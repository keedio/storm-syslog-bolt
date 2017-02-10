package org.keedio.storm.bolt;

public class ConfigurationException extends RuntimeException {

	private static final long serialVersionUID = 2856417661565485907L;

	public ConfigurationException() {
		super();
	}

	public ConfigurationException(String message) {
		super(message);
	}
}
