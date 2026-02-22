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
import java.util.concurrent.TimeUnit;

public class MiniDFSClusterExperiment {
	private static final int DEFAULT_MAX_DATANODES = 4096;
	private static final long ITERATION_PAUSE_MS = 5_000;

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
			System.out.println("  Pre-start: threads=" + Thread.activeCount()
				+ "  heapUsed=" + ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024*1024)) + "MB"
				+ "  heapMax=" + (Runtime.getRuntime().maxMemory() / (1024*1024)) + "MB");
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

			// Force GC and give daemon threads time to die between iterations
			System.gc();
			Thread.sleep(ITERATION_PAUSE_MS);
			System.gc();
			Thread.sleep(1_000);

			// Log active thread count to track thread leaks
			int activeThreads = Thread.activeCount();
			System.out.println("Active threads after cleanup: " + activeThreads);
			if (activeThreads > 25_000) {
				System.err.println("WARNING: Thread count (" + activeThreads + ") approaching ulimit. Stopping to avoid crash.");
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

		// ====================================================================
		// Resource-reduction settings to maximize DataNode count.
		// Machine limits: ulimit -u 31314, ulimit -n 1024
		// Each DN spawns ~10-15 threads (with these settings) and ~5-8 FDs.
		// ====================================================================

		// --- Disable all background scanners (saves 2-4 threads per DN) ---
		conf.setLong("dfs.block.scanner.volume.bytes.per.second", 0);
		conf.setLong("dfs.datanode.directoryscan.interval", -1);

		// --- Minimize handler / transfer threads per DN ---
		conf.setInt("dfs.datanode.handler.count", 1);          // IPC handlers (default 10)
		conf.setInt("dfs.datanode.max.transfer.threads", 1);    // Xceiver threads (default 4096)

		// --- Minimize NameNode threads ---
		conf.setInt("dfs.namenode.handler.count", 2);
		conf.setInt("dfs.namenode.service.handler.count", 1);
		conf.setInt("dfs.namenode.lifeline.handler.count", 1);

		// --- Reduce heartbeat / replication overhead ---
		conf.setLong("dfs.heartbeat.interval", 30);             // less frequent heartbeats
		conf.setInt("dfs.replication", 1);                       // single replica
		conf.setInt("dfs.namenode.replication.min", 1);

		// --- Reduce Jetty / HTTP threads per DN ---
		conf.setInt("dfs.datanode.http.internal-proxy.port", 0);

		// --- Avoid forking 'du' shell process per volume (the OOM trigger!) ---
		// Set a very long refresh interval so DU doesn't spawn threads frequently.
		conf.setLong("fs.du.interval", 600_000_000);  // ~7 days (effectively disabled)
		// Also reduce the CachingGetSpaceUsed jitter
		conf.setLong("fs.getspaceused.jitterMillis", 0);

		// --- Storage: 1 dir per DN (halves FD usage vs default 2) ---
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		builder.numDataNodes(numDataNodes);
		builder.storagesPerDatanode(1);
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
