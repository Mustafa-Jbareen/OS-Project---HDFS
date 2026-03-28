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
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * MiniDFSCluster memory scaling experiment.
 *
 * <p>Spins up a {@link MiniDFSCluster} with increasing DataNode counts
 * (2, 4, 8, …) and records heap memory usage at each step.</p>
 *
 * <h3>Why memory dips can appear between data points</h3>
 * <ol>
 *   <li><b>Non-deterministic GC</b>: {@code System.gc()} is a <em>hint</em>.
 *       The JVM may collect varying amounts of garbage between iterations,
 *       so stale objects from a previous cluster sometimes inflate the
 *       reading while other times they get collected → dip.</li>
 *   <li><b>Heap de-commit</b>: G1GC can give committed heap pages back to
 *       the OS, making {@code Runtime.totalMemory()} shrink between
 *       iterations.</li>
 *   <li><b>Lazy-init caches</b>: Hadoop's internal caches (DNS, class meta,
 *       security tokens) are populated on first use and may be partially
 *       GC'd when a cluster shuts down.</li>
 *   <li><b>Thread-local storage</b>: IPC/Netty thread-locals may survive
 *       across iterations unpredictably.</li>
 * </ol>
 * <p>To mitigate this, we now call GC repeatedly and wait for the heap
 * reading to <em>converge</em> (within 1 MiB) before recording a sample.</p>
 *
 * <p><b>Note:</b> In the current experiment each iteration creates only
 * <em>1 small file → 1 block</em> in HDFS (replication=1). Therefore the
 * number of blocks per DataNode is always 1 / numDataNodes — nearly zero
 * for large clusters. This measures the per-DataNode <em>infrastructure</em>
 * overhead (threads, sockets, scanners) rather than block-metadata
 * overhead. See {@link MiniDFSFixedBlocksExperiment} for a
 * fixed-total-blocks variant.</p>
 */
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

						// ============================================================
			// Aggressive cleanup between iterations.
			// At high DN counts, Hadoop/Netty daemon threads may linger
			// after cluster.shutdown(). We actively kill them so they
			// don't accumulate and exhaust ulimit -u at the next iteration.
			// ============================================================
			interruptLeakedThreads();

			// Scale the pause with the number of DataNodes
			long pauseMs = numDataNodes >= 1024 ? 15_000
					     : numDataNodes >= 256  ? 10_000
					     : ITERATION_PAUSE_MS;

			for (int g = 0; g < 5; g++) {
				System.gc();
				Thread.sleep(1_000);
			}
			Thread.sleep(pauseMs);

			// Second wave: catch stragglers that survived the first interrupt
			interruptLeakedThreads();
			System.gc();
			Thread.sleep(2_000);

			// Log active thread count to track thread leaks
			int activeThreads = Thread.activeCount();
			System.out.println("Active threads after cleanup: " + activeThreads);
			if (activeThreads > 28_000) {
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
		// Resource-reduction settings to maximise DataNode count up to 4096.
		//
		// Thread budget at 4096 DNs (target: stay under ulimit -u ~31000):
		//   Per DN: IPC(1) + Xceiver(1) + BPServiceActor(1) + PacketResponder(0)
		//           + Netty(1 via JVM flag) + DataXceiverServer(1)
		//           = ~5 threads/DN
		//   4096 DNs x 5 = ~20,480 threads
		//   NameNode:      ~50 threads
		//   JVM internal:  ~50 threads
		//   Total:         ~20,580 threads  -- fits within 31k limit.
		//
		// FD budget at 4096 DNs (target: stay under ulimit -n ~65536):
		//   Per DN: ~4-6 FDs (storage dir + sockets)
		//   4096 x 6 = ~24,576 FDs -- fits within 65k limit.
		// ====================================================================

		// --- Disable ALL background scanners (saves 2-4 threads per DN) ---
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
		// At 4096 DNs with 3s heartbeats the NN processes ~1365 heartbeats/sec.
		// Push interval to 120s at high DN counts so it processes ~34/sec instead.
		conf.setLong("dfs.heartbeat.interval", numDataNodes >= 512 ? 120 : 30);
		conf.setInt("dfs.replication", 1);                       // single replica
		conf.setInt("dfs.namenode.replication.min", 1);

		// --- Minimise Jetty (HTTP) thread pool per DataNode (default ~200) ---
		// Combined with -Dio.netty.eventLoopThreads=1 JVM flag this reduces
		// HTTP-related threads from ~18/DN to ~2/DN.
		conf.setInt("hadoop.http.max.threads", 1);
		conf.set("dfs.datanode.http.address", "127.0.0.1:0");
		conf.set("dfs.datanode.https.address", "127.0.0.1:0");
		conf.setInt("dfs.datanode.http.internal-proxy.port", 0);

		// --- Avoid forking 'du' shell process per volume (the OOM trigger!) ---
		conf.setLong("fs.du.interval", 600_000_000);  // ~7 days (effectively disabled)
		conf.setLong("fs.getspaceused.jitterMillis", 0);

		// --- Disable erasure coding background threads ---
		conf.setInt("dfs.namenode.ec.reconstruction.threads", 0);

		// --- Reduce NameNode redundancy monitor interval (saves thread wake-ups) ---
		conf.setLong("dfs.namenode.redundancy.interval.seconds", 600);

		// --- Disable NameNode decommission monitor (not needed in experiment) ---
		conf.setInt("dfs.namenode.decommission.interval", 600);

		// --- Disable DataNode peer caching (saves FDs at scale) ---
		conf.setInt("dfs.client.socketcache.capacity", 0);

		// --- Reduce NameNode block-report processing threads ---
		conf.setInt("dfs.namenode.block.report.queue.size", 1024);

		// --- Increase NameNode stale-datanode detection window ---
		conf.setLong("dfs.namenode.stale.datanode.interval", 600_000);

		// --- Speed up lease recovery (helps cleanup between iterations) ---
		conf.setLong("dfs.namenode.lease-recheck-interval-ms", 60_000);

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

			long usedMemory = stableUsedMemory();
			samples.add(new MemorySample(numDataNodes, usedMemory));
			System.out.println("Memory used with " + numDataNodes + " DataNodes: " + usedMemory + " bytes (" + (usedMemory / (1024*1024)) + " MiB)");

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

		/**
	 * Force several GC cycles and return the used-heap reading once it
	 * converges (within 1 MiB between successive cycles). This greatly
	 * reduces the non-deterministic dips caused by partial GC.
	 * At high DN counts we allow more iterations for convergence.
	 */
	private static long stableUsedMemory() throws InterruptedException {
		long prev = Long.MAX_VALUE;
		for (int i = 0; i < 8; i++) {
			System.gc();
			Thread.sleep(800);
			long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			if (Math.abs(used - prev) < 1024 * 1024) {
				return used; // converged
			}
			prev = used;
		}
		return prev;
	}

	/**
	 * Interrupt and discard leaked daemon threads from Hadoop/Netty that
	 * survive after {@code MiniDFSCluster.shutdown()}.
	 *
	 * <p>Threads like IPC Client connections, BPServiceActor stragglers,
	 * Netty Shutdown threads, Timer threads from Hadoop metrics, etc.
	 * may linger as daemon threads. At 512+ DNs per iteration, these
	 * accumulate across iterations and exhaust the OS thread limit.
	 * Interrupting them forces their run-loops to exit.</p>
	 */
	private static void interruptLeakedThreads() {
		Set<Thread> threads = Thread.getAllStackTraces().keySet();
		Thread current = Thread.currentThread();
		int interrupted = 0;
		for (Thread t : threads) {
			if (t == current || !t.isDaemon() || !t.isAlive()) continue;
			String name = t.getName();
			// Keep JVM-internal threads alive
			if (name.startsWith("Reference ") || name.startsWith("Finalizer")
					|| name.startsWith("Signal ") || name.startsWith("Common-Cleaner")
					|| name.startsWith("Attach ") || name.contains("GC")
					|| name.startsWith("C1 ") || name.startsWith("C2 ")
					|| name.startsWith("Compiler") || name.startsWith("Service Thread")
					|| name.startsWith("Notification ") || name.equals("main")) {
				continue;
			}
			try {
				t.interrupt();
				interrupted++;
			} catch (SecurityException ignored) { }
		}
		if (interrupted > 0) {
			System.out.println("Interrupted " + interrupted + " leaked daemon threads.");
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
