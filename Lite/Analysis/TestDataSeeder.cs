using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Seeds DuckDB with synthetic data for controlled analysis testing.
/// Each scenario method clears test data and inserts known values
/// so engine output is deterministic and verifiable.
/// Only available when analysis is enabled.
/// </summary>
public class TestDataSeeder
{
    private readonly DuckDbInitializer _duckDb;

    /// <summary>
    /// Negative server_id to avoid collisions with real servers (hash-based positive IDs).
    /// </summary>
    public const int TestServerId = -999;
    public const string TestServerName = "TestServer-ErikAI";

    /// <summary>
    /// Test scenarios use a 4-hour window ending "now" so the data
    /// falls within any reasonable time range query.
    /// Captured once so all references use identical boundaries.
    /// </summary>
    private static readonly DateTime _periodEnd = DateTime.UtcNow;
    public static DateTime TestPeriodEnd => _periodEnd;
    public static DateTime TestPeriodStart => _periodEnd.AddHours(-4);
    public static double TestPeriodDurationMs => (TestPeriodEnd - TestPeriodStart).TotalMilliseconds;

    private long _nextId = -1_000_000;

    public TestDataSeeder(DuckDbInitializer duckDb)
    {
        _duckDb = duckDb;
    }

    /// <summary>
    /// Builds an AnalysisContext matching the test data time range.
    /// </summary>
    public static AnalysisContext CreateTestContext()
    {
        return new AnalysisContext
        {
            ServerId = TestServerId,
            ServerName = TestServerName,
            TimeRangeStart = TestPeriodStart,
            TimeRangeEnd = TestPeriodEnd
        };
    }

    /// <summary>
    /// Memory-starved server: high PAGEIOLATCH, moderate SOS, some CXPACKET.
    /// Buffer pool undersized, max memory misconfigured.
    ///
    /// Expected stories:
    ///   PAGEIOLATCH_SH → buffer_pool → max_memory → physical_memory
    ///
    /// Wait fractions (of 4-hour period = 14,400,000 ms):
    ///   PAGEIOLATCH_SH:        10,000,000 ms = 69.4%
    ///   SOS_SCHEDULER_YIELD:    3,000,000 ms = 20.8%
    ///   CXPACKET:               1,500,000 ms = 10.4%
    ///   WRITELOG:                  200,000 ms =  1.4%
    /// </summary>
    public async Task SeedMemoryStarvedServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["PAGEIOLATCH_SH"]      = (10_000_000, 5_000_000, 100_000),
            ["PAGEIOLATCH_EX"]      = (   500_000,   200_000,  10_000),
            ["SOS_SCHEDULER_YIELD"] = ( 3_000_000, 8_000_000,       0),
            ["CXPACKET"]            = ( 1_500_000, 2_000_000,       0),
            ["WRITELOG"]            = (   200_000,   100_000,  20_000),
        };

        await SeedWaitStatsAsync(waits);
        await SeedServerConfigAsync(ctfp: 50, maxdop: 8, maxMemoryMb: 57344);
        await SeedMemoryStatsAsync(totalPhysicalMb: 65_536, bufferPoolMb: 56_000, targetMb: 57_344);
        await SeedFileSizeAsync(totalDataSizeMb: 512_000); // 500GB data on 64GB RAM
        await SeedServerEditionAsync(edition: 2, majorVersion: 16); // Standard 2022
    }

    /// <summary>
    /// Bad parallelism config: CTFP=5, MAXDOP=0, high CX and SOS waits.
    ///
    /// Expected stories:
    ///   CXPACKET → parallelism_config → CTFP(5), MAXDOP(0)
    ///
    /// Wait fractions (of 4-hour period):
    ///   CXPACKET:               8,000,000 ms = 55.6%
    ///   SOS_SCHEDULER_YIELD:    6,000,000 ms = 41.7%
    ///   CXCONSUMER:             2,000,000 ms = 13.9%
    /// </summary>
    public async Task SeedBadParallelismServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["CXPACKET"]            = (8_000_000, 4_000_000,       0),
            ["SOS_SCHEDULER_YIELD"] = (6_000_000, 12_000_000,      0),
            ["CXCONSUMER"]          = (2_000_000, 1_000_000,       0),
            ["THREADPOOL"]          = (   50_000,       20,        0),
        };

        await SeedWaitStatsAsync(waits);
        await SeedServerConfigAsync(ctfp: 5, maxdop: 0); // Bad defaults
        await SeedMemoryStatsAsync(totalPhysicalMb: 131_072, bufferPoolMb: 122_880, targetMb: 122_880);
        await SeedFileSizeAsync(totalDataSizeMb: 204_800); // 200GB
        await SeedServerEditionAsync(edition: 3, majorVersion: 16); // Enterprise 2022
    }

    /// <summary>
    /// Clean server: low waits across the board. Should produce only absolution.
    ///
    /// Wait fractions (of 4-hour period):
    ///   SOS_SCHEDULER_YIELD:      100,000 ms = 0.7%
    ///   WRITELOG:                   50,000 ms = 0.3%
    ///   PAGEIOLATCH_SH:            30,000 ms = 0.2%
    /// </summary>
    public async Task SeedCleanServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["SOS_SCHEDULER_YIELD"] = (100_000, 500_000,     0),
            ["WRITELOG"]            = ( 50_000,  30_000, 5_000),
            ["PAGEIOLATCH_SH"]      = ( 30_000,  15_000, 1_000),
        };

        await SeedWaitStatsAsync(waits);
        await SeedServerConfigAsync(ctfp: 50, maxdop: 8, maxMemoryMb: 122_880);
        await SeedMemoryStatsAsync(totalPhysicalMb: 131_072, bufferPoolMb: 100_000, targetMb: 122_880);
        await SeedFileSizeAsync(totalDataSizeMb: 102_400); // 100GB
        await SeedServerEditionAsync(edition: 3, majorVersion: 16); // Enterprise 2022
    }

    /// <summary>
    /// Thread exhaustion: THREADPOOL dominant with CXPACKET as root cause.
    /// The "emergency — connect via DAC" scenario. Parallel queries consumed
    /// the entire worker thread pool.
    ///
    /// Expected stories:
    ///   THREADPOOL → CXPACKET → SOS_SCHEDULER_YIELD
    ///
    /// Wait fractions (of 4-hour period):
    ///   THREADPOOL:              5,400,000 ms = 37.5%  (avg 270s/wait — severe)
    ///   CXPACKET:                5,000,000 ms = 34.7%
    ///   SOS_SCHEDULER_YIELD:     4,000,000 ms = 27.8%
    ///   CXCONSUMER:              1,000,000 ms =  6.9%
    /// </summary>
    public async Task SeedThreadExhaustionServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["THREADPOOL"]          = (5_400_000,     4_000,       0), // avg 1350ms/wait, >1h and >1s floors
            ["CXPACKET"]            = (5_000_000, 3_000_000,       0),
            ["SOS_SCHEDULER_YIELD"] = (4_000_000, 9_000_000,       0),
            ["CXCONSUMER"]          = (1_000_000,   500_000,       0),
        };

        await SeedWaitStatsAsync(waits);
    }

    /// <summary>
    /// Blocking-driven thread exhaustion: THREADPOOL caused by heavy lock contention.
    /// Stuck queries holding exclusive locks, consuming all available worker threads.
    /// Unlike the parallelism scenario, this is caused by blocking, not DOP.
    ///
    /// Expected stories:
    ///   THREADPOOL → LCK (blocking holding threads)
    ///
    /// Wait fractions (of 4-hour period):
    ///   THREADPOOL:              5,400,000 ms = 37.5%  (avg 270s/wait — severe)
    ///   LCK_M_X:                 4,000,000 ms = 27.8%
    ///   LCK_M_U:                 2,000,000 ms = 13.9%
    ///   LCK_M_IX:                  800,000 ms =  5.6%
    ///   SOS_SCHEDULER_YIELD:       500,000 ms =  3.5%
    /// </summary>
    public async Task SeedBlockingThreadExhaustionServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["THREADPOOL"]          = (5_400_000,     4_000,       0), // avg 1350ms/wait, >1h and >1s floors
            ["LCK_M_X"]            = (4_000_000,   300_000,  50_000),
            ["LCK_M_U"]            = (2_000_000,   150_000,  25_000),
            ["LCK_M_IX"]           = (  800_000,   400_000,  10_000),
            ["SOS_SCHEDULER_YIELD"] = (  500_000, 2_000_000,       0),
        };

        await SeedWaitStatsAsync(waits);
        // 200 blocking events (~50/hr) — heavy, at critical threshold
        await SeedBlockingEventsAsync(200, avgWaitTimeMs: 60_000, sleepingBlockerCount: 40, distinctBlockers: 8);
        // 15 deadlocks (~3.75/hr) — escalating
        await SeedDeadlocksAsync(15);
    }

    /// <summary>
    /// Heavy lock contention: LCK_M_X and LCK_M_U dominant.
    /// Writers blocking writers — classic OLTP contention pattern.
    ///
    /// Expected stories:
    ///   LCK_M_X (exclusive lock waits, highest)
    ///   LCK_M_U (update lock waits)
    ///
    /// Wait fractions (of 4-hour period):
    ///   LCK_M_X:                 3,000,000 ms = 20.8%
    ///   LCK_M_U:                 1,500,000 ms = 10.4%
    ///   LCK_M_IX:                  800,000 ms =  5.6%
    ///   SOS_SCHEDULER_YIELD:       500,000 ms =  3.5%
    ///   WRITELOG:                   400,000 ms =  2.8%
    /// </summary>
    public async Task SeedLockContentionServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["LCK_M_X"]            = (3_000_000,   200_000,  50_000),
            ["LCK_M_U"]            = (1_500_000,   100_000,  25_000),
            ["LCK_M_IX"]           = (  800_000,   300_000,  10_000),
            ["SOS_SCHEDULER_YIELD"] = (  500_000, 2_000_000,       0),
            ["WRITELOG"]            = (  400_000,   200_000,  30_000),
        };

        await SeedWaitStatsAsync(waits);
        // 60 blocking events (~15/hr) — confirmed write-write blocking
        await SeedBlockingEventsAsync(60, avgWaitTimeMs: 30_000, sleepingBlockerCount: 5, distinctBlockers: 4);
    }

    /// <summary>
    /// Reader/writer blocking: LCK_M_S and LCK_M_IS dominant.
    /// Readers blocked by writers — the "enable RCSI" scenario.
    ///
    /// Expected stories:
    ///   LCK_M_S → recommendation to enable RCSI
    ///   LCK_M_IS
    ///
    /// Wait fractions (of 4-hour period):
    ///   LCK_M_S:                 4,000,000 ms = 27.8%
    ///   LCK_M_IS:                2,000,000 ms = 13.9%
    ///   LCK_M_X:                   500,000 ms =  3.5%
    ///   WRITELOG:                   300,000 ms =  2.1%
    /// </summary>
    public async Task SeedReaderWriterBlockingServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["LCK_M_S"]  = (4_000_000,   800_000,  40_000),
            ["LCK_M_IS"] = (2_000_000,   500_000,  20_000),
            ["LCK_M_X"]  = (  500_000,    30_000,   5_000),
            ["WRITELOG"]  = (  300_000,   150_000,  25_000),
        };

        await SeedWaitStatsAsync(waits);
        // 40 blocking events (~10/hr) — reader/writer blocking confirmed
        await SeedBlockingEventsAsync(40, avgWaitTimeMs: 20_000, sleepingBlockerCount: 3, distinctBlockers: 6);
        // 8 deadlocks (~2/hr) — reader/writer deadlocks (RCSI would eliminate)
        await SeedDeadlocksAsync(8);
    }

    /// <summary>
    /// Serializable isolation abuse: range lock modes present.
    /// Someone has SERIALIZABLE on a high-traffic table — unnecessary and destructive.
    ///
    /// Expected stories:
    ///   LCK_M_RIn_X → "SERIALIZABLE or REPEATABLE READ isolation"
    ///   LCK_M_RS_S
    ///
    /// Wait fractions (of 4-hour period):
    ///   LCK_M_RIn_X:              800,000 ms =  5.6%
    ///   LCK_M_RS_S:               600,000 ms =  4.2%
    ///   LCK_M_RIn_S:              400,000 ms =  2.8%
    ///   LCK_M_S:                   200,000 ms =  1.4%
    ///   LCK_M_X:                   100,000 ms =  0.7%
    /// </summary>
    public async Task SeedSerializableAbuseServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["LCK_M_RIn_X"] = (800_000,  50_000,  5_000),
            ["LCK_M_RS_S"]  = (600_000,  40_000,  3_000),
            ["LCK_M_RIn_S"] = (400_000,  30_000,  2_000),
            ["LCK_M_S"]     = (200_000,  60_000,  1_000),
            ["LCK_M_X"]     = (100_000,  10_000,    500),
        };

        await SeedWaitStatsAsync(waits);
        // 25 deadlocks (~6.25/hr) — serializable often causes deadlocks
        await SeedDeadlocksAsync(25);
    }

    /// <summary>
    /// Log write pressure: WRITELOG dominant with some lock contention.
    /// Storage can't keep up with transaction log writes — shared storage
    /// or undersized log disks.
    ///
    /// Expected stories:
    ///   WRITELOG → log write latency
    ///
    /// Wait fractions (of 4-hour period):
    ///   WRITELOG:                 5,000,000 ms = 34.7%
    ///   LCK_M_X:                   600,000 ms =  4.2%
    ///   SOS_SCHEDULER_YIELD:       400,000 ms =  2.8%
    /// </summary>
    public async Task SeedLogWritePressureServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["WRITELOG"]            = (5_000_000, 2_000_000, 500_000),
            ["LCK_M_X"]            = (  600_000,    40_000,   5_000),
            ["SOS_SCHEDULER_YIELD"] = (  400_000, 1_500_000,       0),
        };

        await SeedWaitStatsAsync(waits);
    }

    /// <summary>
    /// Resource semaphore cascade: memory grant waits causing buffer pool
    /// starvation and downstream PAGEIOLATCH. Queries requesting too much memory.
    ///
    /// Expected stories:
    ///   RESOURCE_SEMAPHORE → PAGEIOLATCH_SH (cascade)
    ///
    /// Wait fractions (of 4-hour period):
    ///   RESOURCE_SEMAPHORE:      1,500,000 ms = 10.4%
    ///   PAGEIOLATCH_SH:          6,000,000 ms = 41.7%
    ///   PAGEIOLATCH_EX:            500_000 ms =  3.5%
    ///   SOS_SCHEDULER_YIELD:       800,000 ms =  5.6%
    /// </summary>
    public async Task SeedResourceSemaphoreCascadeServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["RESOURCE_SEMAPHORE"]  = (1_500_000,     5_000,       0), // avg 300s/wait — severe
            ["PAGEIOLATCH_SH"]      = (6_000_000, 3_000_000,  50_000),
            ["PAGEIOLATCH_EX"]      = (  500_000,   200_000,  10_000),
            ["SOS_SCHEDULER_YIELD"] = (  800_000, 3_000_000,       0),
        };

        await SeedWaitStatsAsync(waits);
        await SeedServerConfigAsync(ctfp: 50, maxdop: 8, maxMemoryMb: 57_344);
        await SeedMemoryStatsAsync(totalPhysicalMb: 65_536, bufferPoolMb: 40_000, targetMb: 57_344);
        await SeedFileSizeAsync(totalDataSizeMb: 307_200); // 300GB
        await SeedServerEditionAsync(edition: 2, majorVersion: 16); // Standard 2022
    }

    /// <summary>
    /// Everything on fire: multiple high-severity categories competing.
    /// Memory pressure, CPU pressure, parallelism, lock contention, log writes.
    /// Tests that the engine produces multiple stories in priority order.
    ///
    /// Expected stories (multiple, ordered by severity):
    ///   1. PAGEIOLATCH_SH (memory pressure, amplified by SOS)
    ///   2. CXPACKET (parallelism, amplified by SOS + THREADPOOL)
    ///   3. LCK_M_X (lock contention)
    ///   4. WRITELOG (log writes)
    ///
    /// Wait fractions (of 4-hour period):
    ///   PAGEIOLATCH_SH:          8,000,000 ms = 55.6%
    ///   CXPACKET:                6,000,000 ms = 41.7%
    ///   SOS_SCHEDULER_YIELD:     5,000,000 ms = 34.7%
    ///   LCK_M_X:                 2,000,000 ms = 13.9%
    ///   THREADPOOL:              4,000,000 ms = 27.8%
    ///   WRITELOG:                 1,500,000 ms = 10.4%
    ///   RESOURCE_SEMAPHORE:        300,000 ms =  2.1%
    /// </summary>
    public async Task SeedEverythingOnFireServerAsync()
    {
        await ClearTestDataAsync();
        await SeedTestServerAsync();

        var waits = new Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)>
        {
            ["PAGEIOLATCH_SH"]      = (8_000_000, 4_000_000, 100_000),
            ["CXPACKET"]            = (6_000_000, 3_000_000,       0),
            ["SOS_SCHEDULER_YIELD"] = (5_000_000, 10_000_000,      0),
            ["LCK_M_X"]            = (2_000_000,   150_000,  20_000),
            ["THREADPOOL"]          = (4_000_000,     3_000,       0), // avg 1333ms/wait, >1h and >1s floors
            ["WRITELOG"]            = (1_500_000,   700_000, 150_000),
            ["RESOURCE_SEMAPHORE"]  = (  300_000,     1_000,       0), // avg 300s/wait
        };

        await SeedWaitStatsAsync(waits);
        // 100 blocking events (~25/hr) — systemic blocking
        await SeedBlockingEventsAsync(100, avgWaitTimeMs: 40_000, sleepingBlockerCount: 15, distinctBlockers: 10);
        // 30 deadlocks (~7.5/hr) — escalating
        await SeedDeadlocksAsync(30);
        await SeedServerConfigAsync(ctfp: 5, maxdop: 0, maxMemoryMb: 2_147_483_647); // All defaults
        await SeedMemoryStatsAsync(totalPhysicalMb: 65_536, bufferPoolMb: 58_000, targetMb: 65_536);
        await SeedFileSizeAsync(totalDataSizeMb: 1_024_000); // 1TB
        await SeedServerEditionAsync(edition: 2, majorVersion: 15); // Standard 2019
    }

    /// <summary>
    /// Removes all test data across all tables.
    /// </summary>
    internal async Task ClearTestDataAsync()
    {
        var tables = new[]
        {
            "wait_stats", "memory_stats", "server_config", "database_config",
            "cpu_utilization_stats", "file_io_stats", "memory_clerks",
            "query_stats", "procedure_stats", "query_store_stats",
            "query_snapshots", "tempdb_stats", "perfmon_stats",
            "blocked_process_reports", "deadlocks", "memory_grant_stats",
            "waiting_tasks", "servers"
        };

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        foreach (var table in tables)
        {
            try
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $"DELETE FROM {table} WHERE server_id = {TestServerId}";
                await cmd.ExecuteNonQueryAsync();
            }
            catch
            {
                /* Table may not exist yet — that's fine */
            }
        }
    }

    /// <summary>
    /// Registers the test server in the servers table.
    /// </summary>
    internal async Task SeedTestServerAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO servers (server_id, server_name, display_name, use_windows_auth, is_enabled)
VALUES ($1, $2, $3, true, true)";
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "ErikAI Test Server" });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Seeds blocked_process_reports with synthetic blocking events.
    /// </summary>
    internal async Task SeedBlockingEventsAsync(int count, long avgWaitTimeMs,
        int sleepingBlockerCount = 0, int distinctBlockers = 3)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var intervalMinutes = 240.0 / count; // Spread across 4-hour window

        for (var i = 0; i < count; i++)
        {
            var eventTime = TestPeriodStart.AddMinutes(i * intervalMinutes);
            var id = _nextId--;
            var isSleeping = i < sleepingBlockerCount;
            var blockerSpid = 50 + (i % distinctBlockers);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name,
     event_time, blocked_spid, blocking_spid, wait_time_ms,
     lock_mode, blocked_status, blocking_status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";

            cmd.Parameters.Add(new DuckDBParameter { Value = id });
            cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = 100 + i }); // blocked spid
            cmd.Parameters.Add(new DuckDBParameter { Value = blockerSpid });
            cmd.Parameters.Add(new DuckDBParameter { Value = avgWaitTimeMs });
            cmd.Parameters.Add(new DuckDBParameter { Value = "X" });
            cmd.Parameters.Add(new DuckDBParameter { Value = "suspended" });
            cmd.Parameters.Add(new DuckDBParameter { Value = isSleeping ? "sleeping" : "running" });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds deadlocks table with synthetic deadlock events.
    /// </summary>
    internal async Task SeedDeadlocksAsync(int count)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var intervalMinutes = 240.0 / count;

        for (var i = 0; i < count; i++)
        {
            var eventTime = TestPeriodStart.AddMinutes(i * intervalMinutes);
            var id = _nextId--;

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time)
VALUES ($1, $2, $3, $4, $5)";

            cmd.Parameters.Add(new DuckDBParameter { Value = id });
            cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });

            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Seeds wait_stats with the given wait type values.
    /// Distributes data across 16 collection points (every 15 minutes)
    /// so the data looks realistic in trend queries.
    /// </summary>
    internal async Task SeedWaitStatsAsync(
        Dictionary<string, (long waitTimeMs, long waitingTasks, long signalMs)> waits)
    {
        const int collectionPoints = 16;

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        foreach (var (waitType, totals) in waits)
        {
            var deltaWaitPerPoint = totals.waitTimeMs / collectionPoints;
            var deltaTasksPerPoint = totals.waitingTasks / collectionPoints;
            var deltaSignalPerPoint = totals.signalMs / collectionPoints;

            long cumulativeWait = 0;
            long cumulativeTasks = 0;
            long cumulativeSignal = 0;

            for (var i = 0; i < collectionPoints; i++)
            {
                cumulativeWait += deltaWaitPerPoint;
                cumulativeTasks += deltaTasksPerPoint;
                cumulativeSignal += deltaSignalPerPoint;

                var collectionTime = TestPeriodStart.AddMinutes(i * 15);
                var id = _nextId--;

                using var cmd = connection.CreateCommand();
                cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";

                cmd.Parameters.Add(new DuckDBParameter { Value = id });
                cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
                cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
                cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
                cmd.Parameters.Add(new DuckDBParameter { Value = waitType });
                cmd.Parameters.Add(new DuckDBParameter { Value = cumulativeTasks });
                cmd.Parameters.Add(new DuckDBParameter { Value = cumulativeWait });
                cmd.Parameters.Add(new DuckDBParameter { Value = cumulativeSignal });
                cmd.Parameters.Add(new DuckDBParameter { Value = deltaTasksPerPoint });
                cmd.Parameters.Add(new DuckDBParameter { Value = deltaWaitPerPoint });
                cmd.Parameters.Add(new DuckDBParameter { Value = deltaSignalPerPoint });

                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    /// <summary>
    /// Seeds memory_stats with physical memory, buffer pool, and target memory values.
    /// </summary>
    internal async Task SeedMemoryStatsAsync(double totalPhysicalMb, double bufferPoolMb, double targetMb)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";

        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = totalPhysicalMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = totalPhysicalMb - bufferPoolMb }); // available = total - used
        cmd.Parameters.Add(new DuckDBParameter { Value = targetMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = bufferPoolMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = bufferPoolMb });

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Seeds file_io_stats with a total database size entry.
    /// Creates a single "data" file entry representing the total data footprint.
    /// </summary>
    internal async Task SeedFileSizeAsync(double totalDataSizeMb)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, file_name, file_type, size_mb,
     num_of_reads, num_of_writes, read_bytes, write_bytes,
     io_stall_read_ms, io_stall_write_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 0, 0, 0, 0, 0)";

        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "AllDatabases" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "aggregate_data" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "ROWS" });
        cmd.Parameters.Add(new DuckDBParameter { Value = totalDataSizeMb });

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Updates the test server's edition and major version in the servers table.
    /// </summary>
    internal async Task SeedServerEditionAsync(int edition, int majorVersion)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
UPDATE servers
SET sql_engine_edition = $1,
    sql_major_version = $2
WHERE server_id = $3";

        cmd.Parameters.Add(new DuckDBParameter { Value = edition });
        cmd.Parameters.Add(new DuckDBParameter { Value = majorVersion });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Seeds server_config with specific CTFP and MAXDOP values for testing.
    /// </summary>
    internal async Task SeedServerConfigAsync(int ctfp = 50, int maxdop = 8, int maxMemoryMb = 57344)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        var configs = new (string name, int value)[]
        {
            ("cost threshold for parallelism", ctfp),
            ("max degree of parallelism", maxdop),
            ("max server memory (MB)", maxMemoryMb),
            ("max worker threads", 0)
        };

        foreach (var (name, value) in configs)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name,
     value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";

            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestPeriodEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = TestServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = name });
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
            cmd.Parameters.Add(new DuckDBParameter { Value = true });
            cmd.Parameters.Add(new DuckDBParameter { Value = true });

            await cmd.ExecuteNonQueryAsync();
        }
    }
}
