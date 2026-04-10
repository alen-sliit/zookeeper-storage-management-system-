package com.example.zookeeper;

import com.example.zookeeper.server.StorageNode;

import java.io.OutputStream;
import java.io.PrintStream;

/**
 * Default Maven entry point.
 */
public class App {
	private static final String NOISY_LINE =
			"org.apache.zookeeper.ClientCnxn - Got ping response";

	public static void main(String[] args) {
		installConsoleFilter();
		StorageNode.main(args);
	}

	private static void installConsoleFilter() {
		System.setOut(createFilteredStream(System.out));
		System.setErr(createFilteredStream(System.err));
	}

	private static PrintStream createFilteredStream(PrintStream target) {
		return new PrintStream(new OutputStream() {
			private final StringBuilder buffer = new StringBuilder();

			@Override
			public void write(int b) {
				if (b == '\n') {
					flushLine();
					return;
				}
				if (b != '\r') {
					buffer.append((char) b);
				}
			}

			@Override
			public void flush() {
				flushLine();
				target.flush();
			}

			private void flushLine() {
				if (buffer.length() == 0) {
					return;
				}
				String line = buffer.toString();
				buffer.setLength(0);
				if (!line.contains(NOISY_LINE)) {
					target.println(line);
				}
			}
		}, true);
	}
}
