using System.Collections;
using System.Diagnostics;
using System.Reflection;
using IBM.WMQ;

var useColor = Environment.GetEnvironmentVariable("NO_COLOR") is null
    && (!Console.IsOutputRedirected || Environment.GetEnvironmentVariable("FORCE_COLOR") is not null);
var Cyan = useColor ? "\x1b[36m" : "";
var Green = useColor ? "\x1b[32m" : "";
var Yellow = useColor ? "\x1b[33m" : "";
var Bold = useColor ? "\x1b[1m" : "";
var Dim = useColor ? "\x1b[2m" : "";
var Reset = useColor ? "\x1b[0m" : "";

var host = Environment.GetEnvironmentVariable("IBMMQ_HOST") ?? "localhost";
var port = int.Parse(Environment.GetEnvironmentVariable("IBMMQ_PORT") ?? "1414");
var qmgrName = Environment.GetEnvironmentVariable("IBMMQ_QMGR") ?? "QM1";
var channel = Environment.GetEnvironmentVariable("IBMMQ_CHANNEL") ?? "ADMIN.SVRCONN";
var user = Environment.GetEnvironmentVariable("IBMMQ_USER") ?? "admin";
var password = Environment.GetEnvironmentVariable("IBMMQ_PASSWORD") ?? "passw0rd";

var mqClientVersion = typeof(MQQueueManager).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
    ?? typeof(MQQueueManager).Assembly.GetName().Version?.ToString()
    ?? "unknown";
Console.WriteLine($"{Dim}IBMMQDotnetClient {mqClientVersion}{Reset}");

const double MinRunSeconds = 5.0;

if (args.Length < 1)
{
    PrintUsage();
    return 1;
}

var command = args[0].ToLowerInvariant();

return command switch
{
    "send" => RunSend(args),
    "receive" => RunReceive(args),
    "receivesend" => RunReceiveSend(args),
    "benchmark" => RunBenchmark(args),
    _ => PrintUsage()
};

int PrintUsage()
{
    Console.Error.WriteLine("Usage:");
    Console.Error.WriteLine("  mqbench send <queue> <txmode> <durability> [-n <count>] [-c <concurrency>] [-i <iterations>]");
    Console.Error.WriteLine("  mqbench receive <queue> <txmode> <durability> [-n <count>] [-c <concurrency>] [-i <iterations>]");
    Console.Error.WriteLine("  mqbench receivesend <srcqueue> <destqueue> <txmode> <durability> [-n <count>] [-c <concurrency>] [-i <iterations>]");
    Console.Error.WriteLine("  mqbench benchmark [--express] [--durable] [--tx none,receive,atomic] [-n <count>] [-c <concurrency>] [-i <iterations>] [--json <path>]");
    Console.Error.WriteLine();
    Console.Error.WriteLine("Transaction modes: none, receive, atomic");
    Console.Error.WriteLine("Durability: durable, express");
    Console.Error.WriteLine("Count defaults to 150,000 (express) or 15,000 (durable)");
    Console.Error.WriteLine("Concurrency defaults to processor count ({0})", Environment.ProcessorCount);
    Console.Error.WriteLine("Iterations defaults to 3 (must be odd for median)");
    return 1;
}

int ParseCount(string[] a, int startIndex, bool express)
{
    for (var i = startIndex; i < a.Length - 1; i++)
    {
        if (a[i] == "-n")
            return int.Parse(a[i + 1]);
    }
    return express ? 150_000 : 15_000;
}

int ParseConcurrency(string[] a, int startIndex)
{
    for (var i = startIndex; i < a.Length - 1; i++)
    {
        if (a[i] == "-c")
            return int.Parse(a[i + 1]);
    }
    return Environment.ProcessorCount;
}

int ParseIterations(string[] a, int startIndex)
{
    for (var i = startIndex; i < a.Length - 1; i++)
    {
        if (a[i] == "-i")
        {
            var n = int.Parse(a[i + 1]);
            if (n % 2 == 0)
                throw new ArgumentException("Iterations must be odd for median calculation.");
            return n;
        }
    }
    return 3;
}

TxMode ParseTxMode(string s) => s.ToLowerInvariant() switch
{
    "none" => TxMode.None,
    "receive" => TxMode.Receive,
    "atomic" => TxMode.Atomic,
    _ => throw new ArgumentException($"Unknown transaction mode: {s}. Use none, receive, or atomic.")
};

bool ParseExpress(string s) => s.ToLowerInvariant() switch
{
    "durable" => false,
    "express" => true,
    _ => throw new ArgumentException($"Unknown durability: {s}. Use durable or express.")
};

double Median(List<double> values)
{
    values.Sort();
    return values[values.Count / 2];
}

void WarnIfTooFast(double seconds, int count, double rate)
{
    if (seconds >= MinRunSeconds) return;
    var recommended = (int)(rate * MinRunSeconds * 1.2); // 20% headroom
    recommended = (recommended / 10000 + 1) * 10000; // round up to next 10k
    Console.Error.WriteLine($"  {Yellow}⚠ Run took {seconds:F2}s (<{MinRunSeconds}s) — results may be noisy. Try count={recommended:N0}{Reset}");
}

MQQueueManager ConnectQueueManager()
{
    var props = new Hashtable
    {
        { MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_MANAGED },
        { MQC.HOST_NAME_PROPERTY, host },
        { MQC.PORT_PROPERTY, port },
        { MQC.CHANNEL_PROPERTY, channel },
        { MQC.USER_ID_PROPERTY, user },
        { MQC.PASSWORD_PROPERTY, password },
    };
    return new MQQueueManager(qmgrName, props);
}

MQMessage CreateMessage(bool express)
{
    var msg = new MQMessage();
    msg.CharacterSet = 1208; // UTF-8
    msg.Persistence = express ? MQC.MQPER_NOT_PERSISTENT : MQC.MQPER_PERSISTENT;

    var id = Guid.NewGuid();
    var conversationId = Guid.NewGuid();
    var correlationId = Guid.NewGuid();
    var relatedTo = Guid.NewGuid();
    var traceId = Convert.ToHexString(Guid.NewGuid().ToByteArray()).ToLowerInvariant();
    var spanId = Convert.ToHexString(Guid.NewGuid().ToByteArray()[..8]).ToLowerInvariant();
    var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss:ffffff") + " Z";
    var orderId = id.ToString();
    var trackingNumber = "TRACK-" + orderId[..8].ToUpperInvariant();

    msg.SetStringProperty("NServiceBus.MessageId", id.ToString());
    msg.SetStringProperty("NServiceBus.ConversationId", conversationId.ToString());
    msg.SetStringProperty("NServiceBus.CorrelationId", correlationId.ToString());
    msg.SetStringProperty("NServiceBus.RelatedTo", relatedTo.ToString());
    msg.SetStringProperty("NServiceBus.ContentType", "application/json");
    msg.SetStringProperty("NServiceBus.EnclosedMessageTypes", "Acme.OrderShipped, Acme.Shared, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
    msg.SetStringProperty("NServiceBus.MessageIntent", "Publish");
    msg.SetStringProperty("NServiceBus.OpenTelemetry.StartNewTrace", "True");
    msg.SetStringProperty("NServiceBus.OriginatingEndpoint", "mqbench");
    msg.SetStringProperty("NServiceBus.OriginatingMachine", Environment.MachineName);
    msg.SetStringProperty("NServiceBus.ReplyToAddress", "mqbench");
    msg.SetStringProperty("NServiceBus.TimeSent", timestamp);
    msg.SetStringProperty("NServiceBus.Version", "10.1.0");
    msg.SetStringProperty("traceparent", $"00-{traceId}-{spanId}-01");
    msg.SetStringProperty("$.diagnostics.originating.hostid", Convert.ToHexString(Guid.NewGuid().ToByteArray()).ToLowerInvariant());

    if (express)
    {
        msg.SetStringProperty("NServiceBus.NonDurableMessage", "true");
    }

    var body = $$"""{"OrderId":"{{orderId}}","TrackingNumber":"{{trackingNumber}}"}""";
    msg.WriteString(body);

    return msg;
}

int RunSend(string[] a)
{
    if (a.Length < 4)
        return PrintUsage();

    var queue = a[1];
    var txMode = ParseTxMode(a[2]);
    var express = ParseExpress(a[3]);
    var count = ParseCount(a, 4, express);
    var concurrency = ParseConcurrency(a, 4);
    var iterations = ParseIterations(a, 4);
    var useSyncpoint = txMode != TxMode.None;

    Console.WriteLine($"{Cyan}Sending {count:N0} messages to {queue} (concurrency={concurrency}, tx={txMode.ToString().ToLowerInvariant()}, {(express ? "express" : "durable")}, {iterations} runs)...{Reset}");

    var rates = new List<double>(iterations);

    for (var iter = 0; iter < iterations; iter++)
    {
        var remaining = count;
        const int batchSize = 1000;
        var sw = Stopwatch.StartNew();

        var tasks = Enumerable.Range(0, concurrency).Select(_ => Task.Run(() =>
        {
            using var qm = ConnectQueueManager();
            using var q = qm.AccessQueue(queue, MQC.MQOO_OUTPUT);
            var pmo = new MQPutMessageOptions
            {
                Options = useSyncpoint ? MQC.MQPMO_SYNCPOINT : MQC.MQPMO_NO_SYNCPOINT
            };

            while (true)
            {
                var grab = Math.Min(Interlocked.Add(ref remaining, -batchSize) + batchSize, batchSize);
                if (grab <= 0)
                    break;

                for (var i = 0; i < grab; i++)
                {
                    var msg = CreateMessage(express);
                    q.Put(msg, pmo);
                    if (useSyncpoint)
                        qm.Commit();
                }
            }
        })).ToArray();

        Task.WhenAll(tasks).GetAwaiter().GetResult();
        sw.Stop();

        var rate = count / sw.Elapsed.TotalSeconds;
        rates.Add(rate);
        Console.WriteLine($"  {Dim}#{iter + 1}:{Reset} {sw.Elapsed.TotalSeconds:F2}s ({Green}{rate:N0} msg/s{Reset})");
        WarnIfTooFast(sw.Elapsed.TotalSeconds, count, rate);
    }

    Console.WriteLine($"{Bold}{Green}Sent {count:N0} — median {Median(rates):N0} msg/s, avg {rates.Average():N0} msg/s{Reset}");
    return 0;
}

int RunReceive(string[] a)
{
    if (a.Length < 4)
        return PrintUsage();

    var queue = a[1];
    var txMode = ParseTxMode(a[2]);
    var express = ParseExpress(a[3]);
    var count = ParseCount(a, 4, express);
    var concurrency = ParseConcurrency(a, 4);
    var iterations = ParseIterations(a, 4);
    var useSyncpoint = txMode != TxMode.None;

    Console.WriteLine($"{Cyan}Receiving {count:N0} messages from {queue} (concurrency={concurrency}, tx={txMode.ToString().ToLowerInvariant()}, {(express ? "express" : "durable")}, {iterations} runs)...{Reset}");

    var rates = new List<double>(iterations);

    for (var iter = 0; iter < iterations; iter++)
    {
        var remaining = count;
        const int batchSize = 1000;
        var sw = Stopwatch.StartNew();

        var tasks = Enumerable.Range(0, concurrency).Select(_ => Task.Run(() =>
        {
            using var qm = ConnectQueueManager();
            using var q = qm.AccessQueue(queue, MQC.MQOO_INPUT_SHARED);
            var gmo = new MQGetMessageOptions
            {
                Options = (useSyncpoint ? MQC.MQGMO_SYNCPOINT : MQC.MQGMO_NO_SYNCPOINT)
                        | MQC.MQGMO_NO_WAIT
                        | MQC.MQGMO_FAIL_IF_QUIESCING
            };

            while (true)
            {
                var grab = Math.Min(Interlocked.Add(ref remaining, -batchSize) + batchSize, batchSize);
                if (grab <= 0)
                    break;

                for (var i = 0; i < grab; i++)
                {
                    var msg = new MQMessage();
                    try
                    {
                        q.Get(msg, gmo);
                    }
                    catch (MQException ex) when (ex.ReasonCode == MQC.MQRC_NO_MSG_AVAILABLE)
                    {
                        // Queue drained early
                        Interlocked.Add(ref remaining, -(grab - i - 1));
                        return;
                    }
                    if (useSyncpoint)
                        qm.Commit();
                }
            }
        })).ToArray();

        Task.WhenAll(tasks).GetAwaiter().GetResult();
        sw.Stop();

        var received = count - Math.Max(0, remaining);
        var rate = received / sw.Elapsed.TotalSeconds;
        rates.Add(rate);
        Console.WriteLine($"  {Dim}#{iter + 1}:{Reset} {sw.Elapsed.TotalSeconds:F2}s ({Green}{rate:N0} msg/s{Reset})");
        WarnIfTooFast(sw.Elapsed.TotalSeconds, count, rate);
    }

    Console.WriteLine($"{Bold}{Green}Received {count:N0} — median {Median(rates):N0} msg/s, avg {rates.Average():N0} msg/s{Reset}");
    return 0;
}

int RunReceiveSend(string[] a)
{
    if (a.Length < 5)
        return PrintUsage();

    var srcQueue = a[1];
    var destQueue = a[2];
    var txMode = ParseTxMode(a[3]);
    var express = ParseExpress(a[4]);
    var count = ParseCount(a, 5, express);
    var concurrency = ParseConcurrency(a, 5);
    var iterations = ParseIterations(a, 5);

    Console.WriteLine($"{Cyan}ReceiveSend {count:N0} messages from {srcQueue} to {destQueue} (concurrency={concurrency}, tx={txMode.ToString().ToLowerInvariant()}, {(express ? "express" : "durable")}, {iterations} runs)...{Reset}");

    var rates = new List<double>(iterations);

    for (var iter = 0; iter < iterations; iter++)
    {
        var remaining = count;
        const int batchSize = 1000;
        var sw = Stopwatch.StartNew();

        var tasks = Enumerable.Range(0, concurrency).Select(_ => Task.Run(() =>
        {
            using var qm = ConnectQueueManager();
            using var srcQ = qm.AccessQueue(srcQueue, MQC.MQOO_INPUT_SHARED);
            using var destQ = qm.AccessQueue(destQueue, MQC.MQOO_OUTPUT);

            var gmo = new MQGetMessageOptions
            {
                Options = (txMode != TxMode.None ? MQC.MQGMO_SYNCPOINT : MQC.MQGMO_NO_SYNCPOINT)
                        | MQC.MQGMO_NO_WAIT
                        | MQC.MQGMO_FAIL_IF_QUIESCING
            };

            var pmo = new MQPutMessageOptions
            {
                Options = txMode == TxMode.Atomic ? MQC.MQPMO_SYNCPOINT : MQC.MQPMO_NO_SYNCPOINT
            };

            while (true)
            {
                var grab = Math.Min(Interlocked.Add(ref remaining, -batchSize) + batchSize, batchSize);
                if (grab <= 0)
                    break;

                for (var i = 0; i < grab; i++)
                {
                    var msg = new MQMessage();
                    try
                    {
                        srcQ.Get(msg, gmo);
                    }
                    catch (MQException ex) when (ex.ReasonCode == MQC.MQRC_NO_MSG_AVAILABLE)
                    {
                        Interlocked.Add(ref remaining, -(grab - i - 1));
                        return;
                    }

                    msg.Persistence = express ? MQC.MQPER_NOT_PERSISTENT : MQC.MQPER_PERSISTENT;
                    destQ.Put(msg, pmo);

                    if (txMode != TxMode.None)
                        qm.Commit();
                }
            }
        })).ToArray();

        Task.WhenAll(tasks).GetAwaiter().GetResult();
        sw.Stop();

        var processed = count - Math.Max(0, remaining);
        var rate = processed / sw.Elapsed.TotalSeconds;
        rates.Add(rate);
        Console.WriteLine($"  {Dim}#{iter + 1}:{Reset} {sw.Elapsed.TotalSeconds:F2}s ({Green}{rate:N0} msg/s{Reset})");
        WarnIfTooFast(sw.Elapsed.TotalSeconds, count, rate);
    }

    Console.WriteLine($"{Bold}{Green}ReceiveSend {count:N0} — median {Median(rates):N0} msg/s, avg {rates.Average():N0} msg/s{Reset}");
    return 0;
}

int RunBenchmark(string[] a)
{
    var queueA = "bench.a";
    var queueB = "bench.b";
    List<TxMode>? txModes = null;
    bool flagDurable = false, flagExpress = false;
    int? explicitCount = null;
    int concurrency = Environment.ProcessorCount;
    int iterations = 3;
    string? jsonPath = null;

    for (var i = 1; i < a.Length; i++)
    {
        switch (a[i])
        {
            case "--queues" when i + 1 < a.Length:
                var parts = a[++i].Split(',');
                queueA = parts[0];
                queueB = parts.Length > 1 ? parts[1] : parts[0] + ".b";
                break;
            case "--tx" when i + 1 < a.Length:
                txModes = a[++i].Split(',').Select(ParseTxMode).Distinct().ToList();
                break;
            case "--durable": flagDurable = true; break;
            case "--express": flagExpress = true; break;
            case "-n" when i + 1 < a.Length: explicitCount = int.Parse(a[++i]); break;
            case "-c" when i + 1 < a.Length: concurrency = int.Parse(a[++i]); break;
            case "-i" when i + 1 < a.Length:
                var n = int.Parse(a[++i]);
                if (n % 2 == 0) throw new ArgumentException("Iterations must be odd for median calculation.");
                iterations = n;
                break;
            case "--json" when i + 1 < a.Length: jsonPath = a[++i]; break;
        }
    }

    txModes ??= [TxMode.None, TxMode.Receive, TxMode.Atomic];

    var durabilities = (flagExpress, flagDurable) switch
    {
        (false, false) => new[] { true, false },
        (true, true) => new[] { true, false },
        (true, false) => new[] { true },
        (false, true) => new[] { false },
    };

    Console.WriteLine($"=== mqbench: concurrency={concurrency}, {iterations} iterations ===");
    Console.WriteLine();

    var allRows = new List<BenchmarkRow>();

    foreach (var express in durabilities)
    {
        foreach (var txMode in txModes)
        {
            var durLabel = express ? "express" : "durable";
            var txLabel = txMode.ToString().ToLowerInvariant();
            var scenarioLabel = $"{durLabel} tx={txLabel}";
            var count = explicitCount ?? (express ? 150_000 : 15_000);
            var useSyncpoint = txMode != TxMode.None;

            Console.WriteLine($"--- {scenarioLabel} ({count:N0} msgs) ---");

            var sendRates = new List<double>(iterations);
            var sendSeconds = new List<double>(iterations);
            var recvSendRates = new List<double>(iterations);
            var recvSendSeconds = new List<double>(iterations);
            var receiveRates = new List<double>(iterations);
            var receiveSeconds = new List<double>(iterations);

            for (var iter = 0; iter < iterations; iter++)
            {
                // Send
                var remaining = count;
                var sw = Stopwatch.StartNew();
                Task.WhenAll(Enumerable.Range(0, concurrency).Select(_ => Task.Run(() =>
                {
                    using var qm = ConnectQueueManager();
                    using var q = qm.AccessQueue(queueA, MQC.MQOO_OUTPUT);
                    var pmo = new MQPutMessageOptions { Options = useSyncpoint ? MQC.MQPMO_SYNCPOINT : MQC.MQPMO_NO_SYNCPOINT };
                    while (true)
                    {
                        var grab = Math.Min(Interlocked.Add(ref remaining, -1000) + 1000, 1000);
                        if (grab <= 0) break;
                        for (var j = 0; j < grab; j++) { q.Put(CreateMessage(express), pmo); if (useSyncpoint) qm.Commit(); }
                    }
                }))).GetAwaiter().GetResult();
                sw.Stop();
                var sendRate = count / sw.Elapsed.TotalSeconds;
                sendRates.Add(sendRate);
                sendSeconds.Add(sw.Elapsed.TotalSeconds);

                // ReceiveSend
                remaining = count;
                sw = Stopwatch.StartNew();
                Task.WhenAll(Enumerable.Range(0, concurrency).Select(_ => Task.Run(() =>
                {
                    using var qm = ConnectQueueManager();
                    using var srcQ = qm.AccessQueue(queueA, MQC.MQOO_INPUT_SHARED);
                    using var destQ = qm.AccessQueue(queueB, MQC.MQOO_OUTPUT);
                    var gmo = new MQGetMessageOptions { Options = (txMode != TxMode.None ? MQC.MQGMO_SYNCPOINT : MQC.MQGMO_NO_SYNCPOINT) | MQC.MQGMO_NO_WAIT | MQC.MQGMO_FAIL_IF_QUIESCING };
                    var pmo = new MQPutMessageOptions { Options = txMode == TxMode.Atomic ? MQC.MQPMO_SYNCPOINT : MQC.MQPMO_NO_SYNCPOINT };
                    while (true)
                    {
                        var grab = Math.Min(Interlocked.Add(ref remaining, -1000) + 1000, 1000);
                        if (grab <= 0) break;
                        for (var j = 0; j < grab; j++)
                        {
                            var msg = new MQMessage();
                            try { srcQ.Get(msg, gmo); } catch (MQException ex) when (ex.ReasonCode == MQC.MQRC_NO_MSG_AVAILABLE) { Interlocked.Add(ref remaining, -(grab - j - 1)); return; }
                            msg.Persistence = express ? MQC.MQPER_NOT_PERSISTENT : MQC.MQPER_PERSISTENT;
                            destQ.Put(msg, pmo);
                            if (txMode != TxMode.None) qm.Commit();
                        }
                    }
                }))).GetAwaiter().GetResult();
                sw.Stop();
                var rsProcessed = count - Math.Max(0, remaining);
                var rsRate = rsProcessed / sw.Elapsed.TotalSeconds;
                recvSendRates.Add(rsRate);
                recvSendSeconds.Add(sw.Elapsed.TotalSeconds);

                // Receive
                remaining = count;
                sw = Stopwatch.StartNew();
                Task.WhenAll(Enumerable.Range(0, concurrency).Select(_ => Task.Run(() =>
                {
                    using var qm = ConnectQueueManager();
                    using var q = qm.AccessQueue(queueB, MQC.MQOO_INPUT_SHARED);
                    var gmo = new MQGetMessageOptions { Options = (useSyncpoint ? MQC.MQGMO_SYNCPOINT : MQC.MQGMO_NO_SYNCPOINT) | MQC.MQGMO_NO_WAIT | MQC.MQGMO_FAIL_IF_QUIESCING };
                    while (true)
                    {
                        var grab = Math.Min(Interlocked.Add(ref remaining, -1000) + 1000, 1000);
                        if (grab <= 0) break;
                        for (var j = 0; j < grab; j++)
                        {
                            var msg = new MQMessage();
                            try { q.Get(msg, gmo); } catch (MQException ex) when (ex.ReasonCode == MQC.MQRC_NO_MSG_AVAILABLE) { Interlocked.Add(ref remaining, -(grab - j - 1)); return; }
                            if (useSyncpoint) qm.Commit();
                        }
                    }
                }))).GetAwaiter().GetResult();
                sw.Stop();
                var recvProcessed = count - Math.Max(0, remaining);
                var recvRate = recvProcessed / sw.Elapsed.TotalSeconds;
                receiveRates.Add(recvRate);
                receiveSeconds.Add(sw.Elapsed.TotalSeconds);

                Console.WriteLine($"  {Dim}#{iter + 1}:{Reset} send {Green}{sendRate:N0}/s{Reset} recvsend {Green}{rsRate:N0}/s{Reset} recv {Green}{recvRate:N0}/s{Reset}");
            }

            allRows.Add(new(scenarioLabel, "Send", Median(sendRates), sendRates.Average(), sendSeconds.Average()));
            allRows.Add(new(scenarioLabel, "RecvSend", Median(recvSendRates), recvSendRates.Average(), recvSendSeconds.Average()));
            allRows.Add(new(scenarioLabel, "Receive", Median(receiveRates), receiveRates.Average(), receiveSeconds.Average()));
            Console.WriteLine();
        }
    }

    // Print summary table
    Console.WriteLine($" | {"Scenario",-19} | {"Phase",-8} | {"Median/s",10} | {"Avg/s",10} | {"Avg Time",10} |");
    Console.WriteLine($" | {new string('-', 19)} | {new string('-', 8)} | {new string('-', 10)} | {new string('-', 10)} | {new string('-', 10)} |");
    string? lastScenario = null;
    foreach (var row in allRows)
    {
        var scenario = row.Scenario == lastScenario ? "" : row.Scenario;
        lastScenario = row.Scenario;
        Console.WriteLine($" | {scenario,-19} | {row.Phase,-8} | {Bold}{Green}{row.MedianRate,10:N0}{Reset} | {row.AvgRate,10:N0} | {row.AvgSeconds,8:F2}s  |");
    }
    Console.WriteLine();

    if (jsonPath != null)
    {
        var jsonResult = new
        {
            timestamp = DateTime.UtcNow.ToString("o"),
            versions = new { ibmmqClient = mqClientVersion },
            concurrency,
            iterations,
            results = allRows.Select(r => new
            {
                scenario = r.Scenario,
                phase = r.Phase,
                medianRate = r.MedianRate,
                avgRate = r.AvgRate,
                avgSeconds = Math.Round(r.AvgSeconds, 2)
            })
        };
        var json = System.Text.Json.JsonSerializer.Serialize(jsonResult, new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(jsonPath, json);
        Console.WriteLine($"{Dim}Results written to {jsonPath}{Reset}");
    }

    return 0;
}

record BenchmarkRow(string Scenario, string Phase, double MedianRate, double AvgRate, double AvgSeconds);

enum TxMode { None, Receive, Atomic }
