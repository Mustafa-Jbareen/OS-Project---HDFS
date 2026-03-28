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

/**
 * MiniDFSCluster experiment: Fixed total data blocks distributed across
 * increasing numbers of DataNodes.
 *
 * <p>This experiment answers: how does NameNode memory scale when you keep the
 * total number of HDFS blocks constant but increase the number of DataNodes
 * that the blocks are distributed across?</p>
 *
 * <h3>Approach</h3>
 * <ul>
 *   <li>A fixed number of small files are created (each file &lt; block size
 *       → 1 block per file, replication=1).</li>
 *   <li>The DataNode count increases (2, 4, 8, …). The NameNode distributes
 *       the fixed set of blocks across the available DataNodes.</li>
 *   <li>Memory is measured after all files are written for each DataNode count.</li>
 * </ul>
 *
 * <h3>Why memory dips may occur in the original experiment</h3>
 * <p>The JVM's {@code System.gc()} is only a hint — the garbage collector
 * may or may not reclaim all dead objects between iterations. Additionally:</p>
 * <ul>
 *   <li>G1GC can de-commit heap regions, so {@code totalMemory()} shrinks
 *       unpredictably between runs.</li>
 *   <li>Hadoop caches (DNS, SecurityManager, ClassLoading) have
 *       non-deterministic lifetimes.</li>
 *   <li>Thread-local storage from Netty/IPC layers may survive across
 *       iterations.</li>
 * </ul>
 * <p>This class mitigates the issue by taking multiple GC passes and
 * measuring the <em>stable</em> heap after warmup.</p>
 *
 * <h3>Usage</h3>
 * <pre>
 *   java -cp ... com.example.MiniDFSFixedBlocksExperiment \
 *        [totalBlocks] [maxDataNodes] [outputCsv]
 * </pre>
 */
public class MiniDFSFixedBlocksExperiment {
	private static final int DEFAULT_TOTAL_BLOCKS = 256;
	private static final int DEFAULT_MAX_DATANODES = 512;
	private static final long ITERATION_PAUSE_MS = 5_000;
	// Each file is exactly this many bytes (well below default 128 MB block size → 1 block each)
	private static final int FILE_SIZE_BYTES = 1024;  // 1 KB

	public static void main(String[] args) throws Exception {
		int totalBlocks = DEFAULT_TOTAL_BLOCKS;
		int maxDataNodes = DEFAULT_MAX_DATANODES;
		File outputFile = new File("fixed_blocks_memory.csv");

		if (args.length >= 1) {
			try {
				totalBlocks = Integer.parseInt(args[0]);
			} catch (NumberFormatException e) {
				System.err.println("Invalid totalBlocks: " + args[0] + ". Using default " + DEFAULT_TOTAL_BLOCKS);
			}
		}
		if (args.length >= 2) {
			try {
				maxDataNodes = Integer.parseInt(args[1]);
			} catch (NumberFormatException e) {
				System.err.println("Invalid maxDataNodes: " + args[1] + ". Using default " + DEFAULT_MAX_DATANODES);
			}
		}
		if (args.length >= 3) {
			outputFile = new File(args[2]);
		}

		if (totalBlocks < 1) {
			System.err.println("totalBlocks must be >= 1. Using 1.");
			totalBlocks = 1;
		}
		if (maxDataNodes < 2) {
			System.err.println("maxDataNodes must be >= 2. Using 2.");
			maxDataNodes = 2;
		}

		System.out.println("==============================================");
		System.out.println("MiniDFS Fixed-Blocks Distribution Experiment");
		System.out.println("==============================================");
		System.out.println("Total blocks (files): " + totalBlocks);
		System.out.println("Max DataNodes:        " + maxDataNodes);
				System.out.println("Output:               " + outputFile.getAbsolutePath());
		System.out.println("PID:                  " + ManagementFactory.getRuntimeMXBean().getName());
		System.out.println("Max heap:             " + (Runtime.getRuntime().maxMemory() / (1024*1024)) + " MiB");
		System.out.println("==============================================");
		System.out.println();

		List<Sample> samples = new ArrayList<>();

		for (int numDataNodes = 512; numDataNodes <= maxDataNodes; numDataNodes *= 2) {
			File baseDir = Files.createTempDirectory("minidfs-fixed-" + numDataNodes + "-").toFile();
			baseDir.deleteOnExit();

			int blocksPerDN = totalBlocks / numDataNodes;
			int remainder = totalBlocks % numDataNodes;
			System.out.println("--- " + numDataNodes + " DataNodes ---");
			System.out.println("  Blocks/DN ≈ " + blocksPerDN + " (remainder " + remainder + ")");
			System.out.println("  Pre-start heap: " + usedMiB() + " MiB");

			try {
				runIteration(numDataNodes, totalBlocks, baseDir, samples);
			} catch (OutOfMemoryError oome) {
				System.err.println("OOM at " + numDataNodes + " DataNodes: " + oome.getMessage());
				break;
			} catch (Exception e) {
				System.err.println("Failed at " + numDataNodes + " DataNodes: " + e.getMessage());
				e.printStackTrace();
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

			// Scale the pause with the number of DataNodes — more threads
			// need more time to die, especially above 256 DNs.
			long pauseMs = numDataNodes >= 1024 ? 15_000
					     : numDataNodes >= 256  ? 10_000
					     : ITERATION_PAUSE_MS;

			for (int g = 0; g < 5; g++) {
				System.gc();
				Thread.sleep(1_000);
			}
			Thread.sleep(pauseMs);

			// Second wave: catch any stragglers that survived the first interrupt
			interruptLeakedThreads();
			System.gc();
			Thread.sleep(2_000);

			int activeThreads = Thread.activeCount();
			System.out.println("  Active threads after cleanup: " + activeThreads);
			if (activeThreads > 28_000) {
				System.err.println("WARNING: thread count (" + activeThreads + ") too high, stopping.");
				break;
			}
		}

		saveSamples(outputFile, samples, totalBlocks);
		System.out.println("\nResults written to " + outputFile.getAbsolutePath());
		System.exit(0);
	}

	private static void runIteration(int numDataNodes, int totalBlocks, File baseDir, List<Sample> samples)
			throws Exception {

		Configuration conf = new Configuration();
		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

		// ====================================================================
		// Resource-reduction settings to maximise DataNode count up to 4096.
		//
		// Thread budget at 4096 DNs (target: stay under ulimit -u ~31000):
		//   Per DN: IPC(1) + Xceiver(1) + BPServiceActor(1) + PacketResponder(0)
		//           + Netty(1 via JVM flag) + DataXceiverServer(1)
		//           = ~5 threads/DN
		//   4096 DNs × 5 = ~20,480 threads
		//   NameNode:      ~50 threads
		//   JVM internal:  ~50 threads
		//   Total:         ~20,580 threads  — fits within 31k limit.
		//
		// FD budget at 4096 DNs (target: stay under ulimit -n ~65536):
		//   Per DN: ~4-6 FDs (storage dir + sockets)
		//   4096 × 6 = ~24,576 FDs — fits within 65k limit.
		//
		// Root cause of EAGAIN at ~512 DNs (without these settings):
		//   Each DataNode's DatanodeHttpServer starts a Netty NioEventLoopGroup.
		//   Default group size = 2 × CPU cores → 16 threads/DN on 8-core machine.
		//   At 512 DNs that is 8192 threads from Netty alone, exceeding ulimit -u.
		//   Fix: -Dio.netty.eventLoopThreads=1 JVM flag (set in run script) caps
		//        every EventLoopGroup in this JVM to exactly 1 thread.
		// ====================================================================

		// --- Disable ALL background scanners (saves 2-4 threads/DN) ---
		conf.setLong("dfs.block.scanner.volume.bytes.per.second", 0);
		conf.setLong("dfs.datanode.directoryscan.interval", -1);

		// --- Minimise IPC/handler threads ---
		conf.setInt("dfs.datanode.handler.count", 1);          // default 10
		conf.setInt("dfs.datanode.max.transfer.threads", 1);    // default 4096
		conf.setInt("dfs.namenode.handler.count", 2);            // default 10
		conf.setInt("dfs.namenode.service.handler.count", 1);    // default 10
		conf.setInt("dfs.namenode.lifeline.handler.count", 1);   // default 1

		// --- Minimise Jetty (HTTP) thread pool per DataNode (default ~200) ---
		// Jetty ThreadPoolBudget requires max > (reserved 1 + connectors 1),
		// so minimum is 4 to avoid "insufficient threads" errors.
		conf.setInt("hadoop.http.max.threads", 4);
		conf.set("dfs.datanode.http.address", "127.0.0.1:0");
		conf.set("dfs.datanode.https.address", "127.0.0.1:0");

		// --- Reduce heartbeat / replication overhead ---
		// At 4096 DNs with 3s heartbeats the NN processes ~1365 heartbeats/sec.
		// Push interval to 120s at high DN counts so it processes ~34/sec instead.
		conf.setLong("dfs.heartbeat.interval", numDataNodes >= 512 ? 120 : 30);
		conf.setInt("dfs.replication", 1);
		conf.setInt("dfs.namenode.replication.min", 1);
		conf.setInt("dfs.datanode.http.internal-proxy.port", 0);

		// --- Avoid forking 'du' shell process per volume (the OOM trigger!) ---
		// Each volume periodically forks a 'du' process. At 4096 DNs that's
		// 4096 shell forks piling up, each consuming a PID from ulimit -u.
		conf.setLong("fs.du.interval", 600_000_000);   // ~7 days
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
		// Prevents NN from marking DNs stale during slow cluster startup
		conf.setLong("dfs.namenode.stale.datanode.interval", 600_000);

		// --- Speed up lease recovery (helps cleanup between iterations) ---
		conf.setLong("dfs.namenode.lease-recheck-interval-ms", 60_000);

		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		builder.numDataNodes(numDataNodes);
		builder.storagesPerDatanode(1);

		MiniDFSCluster cluster = null;
		try {
			cluster = builder.build();
			cluster.waitActive();
			FileSystem fs = cluster.getFileSystem();

			// Write totalBlocks files (each 1 KB → 1 block, replication=1)
			byte[] data = new byte[FILE_SIZE_BYTES];
			for (int i = 0; i < data.length; i++) {
				data[i] = (byte) ('A' + (i % 26));
			}

			System.out.println("  Writing " + totalBlocks + " files (" + FILE_SIZE_BYTES + " B each)...");
			for (int i = 0; i < totalBlocks; i++) {
				Path filePath = new Path("/data/block-" + i + ".dat");
				try (FSDataOutputStream out = fs.create(filePath, (short) 1)) {
					out.write(data);
				}
			}
			System.out.println("  All files written.");

			// Stabilise and measure memory
			long memBefore = stableUsedMemory();

			samples.add(new Sample(
					numDataNodes,
					totalBlocks,
					totalBlocks / numDataNodes,
					memBefore));

			System.out.println("  Memory with " + numDataNodes + " DNs, " + totalBlocks + " blocks: "
					+ (memBefore / (1024 * 1024)) + " MiB");

			// Cleanup HDFS files
			fs.delete(new Path("/data"), true);

		} finally {
			if (cluster != null) {
				cluster.shutdown();
				System.out.println("  Cluster stopped.");
			}
		}
	}

		/**
	 * Force several GC cycles and return the used heap once it stabilises.
	 * At high DN counts we allow more iterations for convergence because
	 * the heap churn from shutting down thousands of threads takes longer
	 * to settle.
	 */
	private static long stableUsedMemory() throws InterruptedException {
		long prev = Long.MAX_VALUE;
		for (int i = 0; i < 8; i++) {
			System.gc();
			Thread.sleep(800);
			long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			if (Math.abs(used - prev) < 1024 * 1024) {
				return used;  // Converged within 1 MiB
			}
			prev = used;
		}
		return prev;
	}

	private static long usedMiB() {
		return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024);
	}

	/**
	 * Interrupt and discard leaked daemon threads from Hadoop/Netty that
	 * survive after {@code MiniDFSCluster.shutdown()}.
	 *
	 * <p>After cluster.shutdown(), threads like:
	 * <ul>
	 *   <li>{@code IPC Client} connection threads</li>
	 *   <li>{@code DataNode: [...]}, {@code BPServiceActor} stragglers</li>
	 *   <li>{@code Netty Shutdown ...}, {@code globalEventExecutor} threads</li>
	 *   <li>{@code Timer-} threads from Hadoop metrics</li>
	 *   <li>{@code ExpiringCache} / {@code Memory} / {@code DeletionService}</li>
	 * </ul>
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
			// Interrupt any Hadoop / Netty / IPC / Timer leaked thread
			try {
				t.interrupt();
				interrupted++;
			} catch (SecurityException ignored) { }
		}
		if (interrupted > 0) {
			System.out.println("  Interrupted " + interrupted + " leaked daemon threads.");
		}
	}

	private static void saveSamples(File outputFile, List<Sample> samples, int totalBlocks) throws IOException {
		File parent = outputFile.getParentFile();
		if (parent != null && !parent.exists() && !parent.mkdirs()) {
			throw new IOException("Unable to create directory " + parent);
		}
		try (PrintWriter w = new PrintWriter(new FileWriter(outputFile))) {
			w.println("DataNodes,TotalBlocks,BlocksPerDataNode,MemoryUsed");
			for (Sample s : samples) {
				w.printf("%d,%d,%d,%d%n",
						s.dataNodes, s.totalBlocks, s.blocksPerDN, s.memoryBytes);
			}
		}
	}

	private static void deleteDirectory(File dir) {
		if (dir == null || !dir.exists()) return;
		File[] children = dir.listFiles();
		if (children != null) {
			for (File child : children) deleteDirectory(child);
		}
		if (!dir.delete()) {
			System.err.println("Warning: failed to delete " + dir);
		}
	}

	private static final class Sample {
		final int dataNodes;
		final int totalBlocks;
		final int blocksPerDN;
		final long memoryBytes;

		Sample(int dataNodes, int totalBlocks, int blocksPerDN, long memoryBytes) {
			this.dataNodes = dataNodes;
			this.totalBlocks = totalBlocks;
			this.blocksPerDN = blocksPerDN;
			this.memoryBytes = memoryBytes;
		}
	}
}
