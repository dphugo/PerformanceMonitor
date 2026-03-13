using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests the DuckDbFactCollector against seeded test data.
/// Verifies that facts are collected with correct values and metadata.
/// </summary>
public class FactCollectorTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _duckDb;

    public FactCollectorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _duckDb = new DuckDbInitializer(_dbPath);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { /* Best-effort cleanup */ }
    }

    [Fact]
    public async Task CollectFacts_MemoryStarvedServer_ReturnsWaitFacts()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        Assert.NotEmpty(facts);
        Assert.Contains(facts, f => f.Source == "waits");
    }

    [Fact]
    public async Task CollectFacts_MemoryStarvedServer_PageioLatchHasCorrectFraction()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var pageioFact = facts.First(f => f.Key == "PAGEIOLATCH_SH");

        /* 10,000,000 ms / 14,400,000 ms ≈ 0.694 */
        Assert.InRange(pageioFact.Value, 0.68, 0.71);
        Assert.Equal(TestDataSeeder.TestServerId, pageioFact.ServerId);
    }

    [Fact]
    public async Task CollectFacts_MemoryStarvedServer_MetadataContainsRawValues()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var pageioFact = facts.First(f => f.Key == "PAGEIOLATCH_SH");

        Assert.True(pageioFact.Metadata.ContainsKey("wait_time_ms"));
        Assert.True(pageioFact.Metadata.ContainsKey("waiting_tasks_count"));
        Assert.True(pageioFact.Metadata.ContainsKey("signal_wait_time_ms"));
        Assert.True(pageioFact.Metadata.ContainsKey("avg_ms_per_wait"));

        /* Raw wait_time_ms should be close to 10,000,000 (integer division may lose some) */
        Assert.InRange(pageioFact.Metadata["wait_time_ms"], 9_900_000, 10_100_000);
    }

    [Fact]
    public async Task CollectFacts_MemoryStarvedServer_WaitsOrderedByValue()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        /* PAGEIOLATCH_SH should be the highest wait */
        var waitFacts = facts.Where(f => f.Source == "waits").ToList();
        Assert.Equal("PAGEIOLATCH_SH", waitFacts[0].Key);
    }

    [Fact]
    public async Task CollectFacts_CleanServer_ReturnsLowFractions()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedCleanServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        /* All waits should be well below 5% of the period */
        var waitFacts = facts.Where(f => f.Source == "waits").ToList();
        Assert.All(waitFacts, f => Assert.True(f.Value < 0.05,
            $"{f.Key} fraction {f.Value:P1} should be < 5%"));
    }

    [Fact]
    public async Task CollectFacts_BadParallelism_CxPacketDominates()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedBadParallelismServerAsync();

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var cxFact = facts.First(f => f.Key == "CXPACKET");
        var sosFact = facts.First(f => f.Key == "SOS_SCHEDULER_YIELD");

        /* CXPACKET should have highest fraction among wait facts (CXPACKET + CXCONSUMER combined) */
        var highest = facts.Where(f => f.Source == "waits").OrderByDescending(f => f.Value).First();
        Assert.Equal("CXPACKET", highest.Key);

        /* (8,000,000 + 2,000,000) / 14,400,000 ≈ 0.694 */
        Assert.InRange(cxFact.Value, 0.68, 0.71);
    }
}
