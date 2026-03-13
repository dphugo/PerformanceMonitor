using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Assigns severity to facts using threshold formulas (Layer 1)
/// and contextual amplifiers (Layer 2).
///
/// Layer 1: Base severity 0.0-1.0 from thresholds alone.
/// Layer 2: Amplifiers multiply base up to 2.0 max using corroborating facts.
///
/// Formula: severity = min(base * (1.0 + sum(amplifiers)), 2.0)
/// </summary>
public class FactScorer
{
    /// <summary>
    /// Scores all facts: Layer 1 (base severity), then Layer 2 (amplifiers).
    /// </summary>
    public void ScoreAll(List<Fact> facts)
    {
        // Layer 1: base severity from thresholds
        foreach (var fact in facts)
        {
            fact.BaseSeverity = fact.Source switch
            {
                "waits" => ScoreWaitFact(fact),
                "blocking" => ScoreBlockingFact(fact),
                _ => 0.0
            };
        }

        // Build lookup for amplifier evaluation (include config facts for context)
        var factsByKey = facts
            .Where(f => f.BaseSeverity > 0 || f.Source == "config")
            .ToDictionary(f => f.Key, f => f);

        // Layer 2: amplifiers boost base severity using corroborating facts
        foreach (var fact in facts)
        {
            if (fact.BaseSeverity <= 0)
            {
                fact.Severity = 0;
                continue;
            }

            var amplifiers = GetAmplifiers(fact);
            var totalBoost = 0.0;

            foreach (var amp in amplifiers)
            {
                var matched = amp.Predicate(factsByKey);
                fact.AmplifierResults.Add(new AmplifierResult
                {
                    Description = amp.Description,
                    Matched = matched,
                    Boost = matched ? amp.Boost : 0.0
                });

                if (matched) totalBoost += amp.Boost;
            }

            fact.Severity = Math.Min(fact.BaseSeverity * (1.0 + totalBoost), 2.0);
        }
    }

    /// <summary>
    /// Scores a wait fact using the fraction-of-period formula.
    /// Some waits have absolute minimum thresholds to filter out background noise.
    /// </summary>
    private static double ScoreWaitFact(Fact fact)
    {
        var fraction = fact.Value;
        if (fraction <= 0) return 0.0;

        // THREADPOOL: require both meaningful total wait time AND meaningful average.
        // Tiny amounts are normal thread pool grow/shrink housekeeping, not exhaustion.
        if (fact.Key == "THREADPOOL")
        {
            var waitTimeMs = fact.Metadata.GetValueOrDefault("wait_time_ms");
            var avgMs = fact.Metadata.GetValueOrDefault("avg_ms_per_wait");
            if (waitTimeMs < 3_600_000 || avgMs < 1_000) return 0.0;
        }

        var thresholds = GetWaitThresholds(fact.Key);
        if (thresholds == null) return 0.0;

        return ApplyThresholdFormula(fraction, thresholds.Value.concerning, thresholds.Value.critical);
    }

    /// <summary>
    /// Scores blocking/deadlock facts using events-per-hour thresholds.
    /// </summary>
    private static double ScoreBlockingFact(Fact fact)
    {
        var value = fact.Value; // events per hour
        if (value <= 0) return 0.0;

        return fact.Key switch
        {
            // Blocking: concerning >10/hr, critical >50/hr
            "BLOCKING_EVENTS" => ApplyThresholdFormula(value, 10, 50),
            // Deadlocks: concerning >5/hr (no critical — any sustained deadlocking is bad)
            "DEADLOCKS" => ApplyThresholdFormula(value, 5, null),
            _ => 0.0
        };
    }

    /// <summary>
    /// Generic threshold formula used by waits, latency, and count-based metrics.
    /// Critical == null means "concerning only" — hitting concerning = 1.0.
    /// </summary>
    internal static double ApplyThresholdFormula(double value, double concerning, double? critical)
    {
        if (value <= 0) return 0.0;

        if (critical == null)
            return Math.Min(value / concerning, 1.0);

        if (value >= critical.Value)
            return 1.0;

        if (value >= concerning)
            return 0.5 + 0.5 * (value - concerning) / (critical.Value - concerning);

        return 0.5 * (value / concerning);
    }

    /// <summary>
    /// Returns amplifier definitions for a fact. Each amplifier has a description,
    /// a boost value, and a predicate that evaluates against the current fact set.
    /// Amplifiers are defined per wait type and will grow as more fact categories are added.
    /// </summary>
    private static List<AmplifierDefinition> GetAmplifiers(Fact fact)
    {
        return fact.Key switch
        {
            "SOS_SCHEDULER_YIELD" => SosSchedulerYieldAmplifiers(),
            "CXPACKET" => CxPacketAmplifiers(),
            "THREADPOOL" => ThreadpoolAmplifiers(),
            "PAGEIOLATCH_SH" or "PAGEIOLATCH_EX" => PageiolatchAmplifiers(),
            "BLOCKING_EVENTS" => BlockingEventsAmplifiers(),
            "DEADLOCKS" => DeadlockAmplifiers(),
            "LCK" => LckAmplifiers(),
            _ => []
        };
    }

    /// <summary>
    /// SOS_SCHEDULER_YIELD: CPU starvation confirmed by parallelism waits.
    /// More amplifiers added when config and CPU utilization facts are available.
    /// </summary>
    private static List<AmplifierDefinition> SosSchedulerYieldAmplifiers() =>
    [
        new()
        {
            Description = "CXPACKET significant — parallelism consuming schedulers",
            Boost = 0.2,
            Predicate = facts => HasSignificantWait(facts, "CXPACKET", 0.10)
        },
        new()
        {
            Description = "THREADPOOL waits present — escalating to thread exhaustion",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("THREADPOOL") && facts["THREADPOOL"].BaseSeverity > 0
        }
    ];

    /// <summary>
    /// CXPACKET: parallelism waits confirmed by CPU pressure and bad config.
    /// CXCONSUMER is grouped into CXPACKET by the collector.
    /// </summary>
    private static List<AmplifierDefinition> CxPacketAmplifiers() =>
    [
        new()
        {
            Description = "SOS_SCHEDULER_YIELD high — CPU starvation from parallelism",
            Boost = 0.3,
            Predicate = facts => HasSignificantWait(facts, "SOS_SCHEDULER_YIELD", 0.25)
        },
        new()
        {
            Description = "THREADPOOL waits present — thread exhaustion cascade",
            Boost = 0.4,
            Predicate = facts => facts.ContainsKey("THREADPOOL") && facts["THREADPOOL"].BaseSeverity > 0
        },
        new()
        {
            Description = "CTFP at default (5) — too low for most workloads",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("CONFIG_CTFP", out var ctfp) && ctfp.Value <= 5
        },
        new()
        {
            Description = "MAXDOP at 0 — unlimited parallelism",
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue("CONFIG_MAXDOP", out var maxdop) && maxdop.Value == 0
        }
    ];

    /// <summary>
    /// THREADPOOL: thread exhaustion confirmed by parallelism pressure.
    /// Blocking and config amplifiers added later.
    /// </summary>
    private static List<AmplifierDefinition> ThreadpoolAmplifiers() =>
    [
        new()
        {
            Description = "CXPACKET significant — parallel queries consuming thread pool",
            Boost = 0.2,
            Predicate = facts => HasSignificantWait(facts, "CXPACKET", 0.10)
        },
        new()
        {
            Description = "Lock contention present — blocked queries holding worker threads",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("LCK") && facts["LCK"].BaseSeverity >= 0.5
        }
    ];

    /// <summary>
    /// PAGEIOLATCH: memory pressure confirmed by other waits.
    /// Buffer pool, query, and config amplifiers added when those facts are available.
    /// </summary>
    private static List<AmplifierDefinition> PageiolatchAmplifiers() =>
    [
        new()
        {
            Description = "SOS_SCHEDULER_YIELD elevated — CPU pressure alongside I/O pressure",
            Boost = 0.1,
            Predicate = facts => HasSignificantWait(facts, "SOS_SCHEDULER_YIELD", 0.15)
        }
    ];

    /// <summary>
    /// BLOCKING_EVENTS: blocking confirmed by lock waits and deadlocks.
    /// </summary>
    private static List<AmplifierDefinition> BlockingEventsAmplifiers() =>
    [
        new()
        {
            Description = "Head blocker sleeping with open transaction — abandoned transaction pattern",
            Boost = 0.4,
            Predicate = facts => facts.TryGetValue("BLOCKING_EVENTS", out var f)
                              && f.Metadata.GetValueOrDefault("sleeping_blocker_count") > 0
        },
        new()
        {
            Description = "Lock contention waits elevated — blocking visible in wait stats",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("LCK") && facts["LCK"].BaseSeverity >= 0.3
        },
        new()
        {
            Description = "Deadlocks also present — blocking escalating to deadlocks",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("DEADLOCKS") && facts["DEADLOCKS"].BaseSeverity > 0
        }
    ];

    /// <summary>
    /// DEADLOCKS: deadlocks confirmed by blocking patterns.
    /// </summary>
    private static List<AmplifierDefinition> DeadlockAmplifiers() =>
    [
        new()
        {
            Description = "Blocking events also present — systemic contention pattern",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("BLOCKING_EVENTS") && facts["BLOCKING_EVENTS"].BaseSeverity > 0
        },
        new()
        {
            Description = "Reader/writer lock waits present — RCSI could prevent some deadlocks",
            Boost = 0.3,
            Predicate = facts => (facts.ContainsKey("LCK_M_S") && facts["LCK_M_S"].BaseSeverity > 0)
                              || (facts.ContainsKey("LCK_M_IS") && facts["LCK_M_IS"].BaseSeverity > 0)
        }
    ];

    /// <summary>
    /// LCK (grouped general lock contention): confirmed by blocking reports and deadlocks.
    /// </summary>
    private static List<AmplifierDefinition> LckAmplifiers() =>
    [
        new()
        {
            Description = "Blocked process reports present — confirmed blocking events",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("BLOCKING_EVENTS") && facts["BLOCKING_EVENTS"].BaseSeverity > 0
        },
        new()
        {
            Description = "Deadlocks present — lock contention escalating to deadlocks",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("DEADLOCKS") && facts["DEADLOCKS"].BaseSeverity > 0
        },
        new()
        {
            Description = "THREADPOOL waits present — blocking causing thread exhaustion",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("THREADPOOL") && facts["THREADPOOL"].BaseSeverity > 0
        }
    ];

    /// <summary>
    /// Checks if a wait type is present with at least the given fraction of period.
    /// </summary>
    private static bool HasSignificantWait(Dictionary<string, Fact> facts, string waitType, double minFraction)
    {
        return facts.TryGetValue(waitType, out var fact) && fact.Value >= minFraction;
    }

    /// <summary>
    /// Default thresholds for wait types (fraction of examined period).
    /// Returns null for unrecognized waits — they get severity 0.
    /// </summary>
    private static (double concerning, double? critical)? GetWaitThresholds(string waitType)
    {
        return waitType switch
        {
            // CPU pressure
            "SOS_SCHEDULER_YIELD" => (0.75, null),
            "THREADPOOL"          => (0.01, null),

            // Memory pressure
            "PAGEIOLATCH_SH"      => (0.25, null),
            "PAGEIOLATCH_EX"      => (0.25, null),
            "RESOURCE_SEMAPHORE"  => (0.01, null),

            // Parallelism (CXCONSUMER is grouped into CXPACKET by collector)
            "CXPACKET"            => (0.25, null),

            // Log I/O
            "WRITELOG"            => (0.10, null),

            // Lock waits — serializable/repeatable read lock modes
            "LCK_M_RS_S"  => (0.01, null),
            "LCK_M_RS_U"  => (0.01, null),
            "LCK_M_RIn_NL" => (0.01, null),
            "LCK_M_RIn_S" => (0.01, null),
            "LCK_M_RIn_U" => (0.01, null),
            "LCK_M_RIn_X" => (0.01, null),
            "LCK_M_RX_S"  => (0.01, null),
            "LCK_M_RX_U"  => (0.01, null),
            "LCK_M_RX_X"  => (0.01, null),

            // Reader/writer blocking locks
            "LCK_M_S"  => (0.05, null),
            "LCK_M_IS" => (0.05, null),

            // General lock contention (grouped X, U, IX, SIX, BU, etc.)
            "LCK" => (0.10, null),

            // Schema locks — DDL operations, index rebuilds
            "SCH_M" => (0.01, null),

            _ => null
        };
    }
}

/// <summary>
/// An amplifier definition: a named predicate that boosts severity when matched.
/// </summary>
internal class AmplifierDefinition
{
    public string Description { get; set; } = string.Empty;
    public double Boost { get; set; }
    public Func<Dictionary<string, Fact>, bool> Predicate { get; set; } = _ => false;
}
