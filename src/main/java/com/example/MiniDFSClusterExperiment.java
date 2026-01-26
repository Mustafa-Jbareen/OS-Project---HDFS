package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class MiniDFSClusterExperiment {
	private static final int DEFAULT_MAX_DATANODES = 32;
	private static final long ITERATION_PAUSE_MS = 2_000;

	public static void main(String[] args) throws Exception {
		int maxDataNodes = DEFAULT_MAX_DATANODES;
		File outputFile = new File("memory_usage.csv");

		if (args.length >= 1) {
			try {
				maxDataNodes = Integer.parseInt(args[0]);
			} catch (NumberFormatException e) {
				System.err.println("Invalid value for max data nodes: " + args[0] + ". Using default " + DEFAULT_MAX_DATANODES);
			}
		}

		if (maxDataNodes < 2) {
			System.err.println("Max data nodes must be at least 2. Using 2.");
			maxDataNodes = 2;
		}

		if (args.length >= 2) {
			outputFile = new File(args[1]);
		}

		List<MemorySample> samples = new ArrayList<>();

		for (int numDataNodes = 2; numDataNodes <= maxDataNodes; numDataNodes *= 2) {
			File baseDir = Files.createTempDirectory("minidfs-" + numDataNodes + "-").toFile();
			baseDir.deleteOnExit();

			System.out.println("Starting MiniDFSCluster with " + numDataNodes + " DataNodes (data dir: " + baseDir.getAbsolutePath() + ")");
			try {
				runIteration(numDataNodes, baseDir, samples);
			} catch (OutOfMemoryError oome) {
				System.err.println("OutOfMemoryError at " + numDataNodes + " DataNodes: " + oome.getMessage());
				break;
			} catch (Exception e) {
				System.err.println("Iteration failed for " + numDataNodes + " DataNodes: " + e.getMessage());
				break;
			} finally {
				deleteDirectory(baseDir);
			}

			try {
				Thread.sleep(ITERATION_PAUSE_MS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				System.err.println("Interrupted between iterations; stopping.");
				break;
			}
		}

		saveSamples(outputFile, samples);
		System.out.println("Memory usage data written to " + outputFile.getAbsolutePath());
		System.exit(0);
	}

	private static void runIteration(int numDataNodes, File baseDir, List<MemorySample> samples) throws Exception {
		Configuration conf = new Configuration();
		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		builder.numDataNodes(numDataNodes);
		MiniDFSCluster cluster = null;
		try {
			cluster = builder.build();
			FileSystem fs = cluster.getFileSystem();
			Path testFile = new Path("/test-file-" + numDataNodes + ".txt");

			try (FSDataOutputStream out = fs.create(testFile)) {
				out.write("Test data".getBytes(StandardCharsets.UTF_8));
			}

			long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			samples.add(new MemorySample(numDataNodes, usedMemory));
			System.out.println("Memory used with " + numDataNodes + " DataNodes: " + usedMemory + " bytes");

			if (!fs.delete(testFile, false)) {
				System.err.println("Warning: failed to delete " + testFile);
			}
		} finally {
			if (cluster != null) {
				cluster.shutdown();
				System.out.println("Cluster with " + numDataNodes + " DataNodes stopped.");
			}
		}
	}

	private static void saveSamples(File outputFile, List<MemorySample> samples) throws IOException {
		File parent = outputFile.getParentFile();
		if (parent != null && !parent.exists() && !parent.mkdirs()) {
			throw new IOException("Unable to create directory " + parent);
		}

		try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
			writer.println("DataNodes,MemoryUsed");
			for (MemorySample sample : samples) {
				writer.printf("%d,%d%n", sample.getDataNodes(), sample.getMemoryBytes());
			}
		}
	}

	private static void deleteDirectory(File dir) {
		if (dir == null || !dir.exists()) {
			return;
		}

		File[] children = dir.listFiles();
		if (children != null) {
			for (File child : children) {
				deleteDirectory(child);
			}
		}

		if (!dir.delete()) {
			System.err.println("Warning: failed to delete temporary directory " + dir);
		}
	}

	private static final class MemorySample {
		private final int dataNodes;
		private final long memoryBytes;

		private MemorySample(int dataNodes, long memoryBytes) {
			this.dataNodes = dataNodes;
			this.memoryBytes = memoryBytes;
		}

		private int getDataNodes() {
			return dataNodes;
		}

		private long getMemoryBytes() {
			return memoryBytes;
		}
	}
}
