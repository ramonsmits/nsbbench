using System.Diagnostics;
using System.Reflection;
using System.Text;
using NServiceBus;
using NServiceBus.Logging;
using NServiceBus.Routing;
using NServiceBus.Transport;
using NServiceBus.Transport.IBMMQ;
using IBM.WMQ;

var useColor = !Console.IsOutputRedirected && Environment.GetEnvironmentVariable("NO_COLOR") is null;
var Cyan = useColor ? "\x1b[36m" : "";
var Green = useColor ? "\x1b[32m" : "";
var Yellow = useColor ? "\x1b[33m" : "";
var Bold = useColor ? "\x1b[1m" : "";
var Dim = useColor ? "\x1b[2m" : "";
var Reset = useColor ? "\x1b[0m" : "";

LogManager.UseFactory(new StderrLoggerFactory());

var coreVersion = typeof(IMessage).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
    ?? typeof(IMessage).Assembly.GetName().Version?.ToString()
    ?? "unknown";
var transportVersion = typeof(IBMMQTransport).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
    ?? typeof(IBMMQTransport).Assembly.GetName().Version?.ToString()
    ?? "unknown";
var mqClientVersion = typeof(MQQueueManager).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
    ?? typeof(MQQueueManager).Assembly.GetName().Version?.ToString()
    ?? "unknown";
Console.WriteLine($"{Dim}NServiceBus {coreVersion}{Reset}");
Console.WriteLine($"{Dim}NServiceBus.Transport.IBMMQ {transportVersion}{Reset}");
Console.WriteLine($"{Dim}IBMMQDotnetClient {mqClientVersion}{Reset}");

var ibmmqHost = Environment.GetEnvironmentVariable("IBMMQ_HOST") ?? "localhost";
var ibmmqPort = int.Parse(Environment.GetEnvironmentVariable("IBMMQ_PORT") ?? "1414");
var qmgrName = Environment.GetEnvironmentVariable("IBMMQ_QMGR") ?? "QM1";
var ibmmqChannel = Environment.GetEnvironmentVariable("IBMMQ_CHANNEL") ?? "ADMIN.SVRCONN";
var ibmmqUser = Environment.GetEnvironmentVariable("IBMMQ_USER") ?? "admin";
var ibmmqPassword = Environment.GetEnvironmentVariable("IBMMQ_PASSWORD") ?? "passw0rd";

const double MinRunSeconds = 5.0;

if (args.Length < 1)
{
    PrintUsage();
    return 1;
}

var command = args[0].ToLowerInvariant();

return await (command switch
{
    "warmup" => RunWarmup(),
    "send" => RunSend(args),
    "receive" => RunReceive(args),
    "receivesend" => RunReceiveSend(args),
    "benchmark" => RunBenchmark(args),
    _ => Task.FromResult(PrintUsage())
});

int PrintUsage()
{
    Console.Error.WriteLine("Usage:");
    Console.Error.WriteLine("  nsbbench warmup");
    Console.Error.WriteLine("  nsbbench send <queue> <txmode> <durability> [-n <count>] [-c <concurrency>] [-i <iterations>]");
    Console.Error.WriteLine("  nsbbench receive <queue> <txmode> <durability> [-n <count>] [-c <concurrency>] [-i <iterations>]");
    Console.Error.WriteLine("  nsbbench receivesend <srcqueue> <destqueue> <txmode> <durability> [-n <count>] [-c <concurrency>] [-i <iterations>]");
    Console.Error.WriteLine("  nsbbench benchmark [options]");
    Console.Error.WriteLine();
    Console.Error.WriteLine("Benchmark options:");
    Console.Error.WriteLine("  --queues <a>,<b>    Queue pair (default: bench.a,bench.b)");
    Console.Error.WriteLine("  --tx <modes>        Comma-separated: none,receive,atomic (default: all)");
    Console.Error.WriteLine("  --durable           Run durable scenarios only");
    Console.Error.WriteLine("  --express           Run express scenarios only");
    Console.Error.WriteLine("                      (omit both for full matrix)");
    Console.Error.WriteLine("  -n <count>          Messages per scenario (default: 150,000 express / 15,000 durable)");
    Console.Error.WriteLine("  -c <concurrency>    Concurrency (default: processor count = {0})", Environment.ProcessorCount);
    Console.Error.WriteLine("  -i <iterations>     Iterations per scenario, must be odd (default: 3)");
    Console.Error.WriteLine();
    Console.Error.WriteLine("Examples:");
    Console.Error.WriteLine("  nsbbench benchmark                              # full matrix");
    Console.Error.WriteLine("  nsbbench benchmark --durable                    # durable only, all tx modes");
    Console.Error.WriteLine("  nsbbench benchmark --tx atomic,receive --durable # subset");
    Console.Error.WriteLine("  nsbbench benchmark --express --tx none -n 100000");
    Console.Error.WriteLine();
    Console.Error.WriteLine("Transaction modes: none, receive, atomic");
    Console.Error.WriteLine("Durability: durable, express");
    Console.Error.WriteLine("Count defaults to 150,000 (express) or 15,000 (durable)");
    Console.Error.WriteLine("Concurrency defaults to processor count ({0})", Environment.ProcessorCount);
    Console.Error.WriteLine("Iterations defaults to 3 (must be odd for median)");
    return 1;
}

// --- Arg parsing ---

int ParseCount(string[] a, int startIndex, bool express)
{
    for (var i = startIndex; i < a.Length - 1; i++)
        if (a[i] == "-n") return int.Parse(a[i + 1]);
    return express ? 150_000 : 15_000;
}

int ParseConcurrency(string[] a, int startIndex)
{
    for (var i = startIndex; i < a.Length - 1; i++)
        if (a[i] == "-c") return int.Parse(a[i + 1]);
    return Environment.ProcessorCount;
}

int ParseIterations(string[] a, int startIndex)
{
    for (var i = startIndex; i < a.Length - 1; i++)
        if (a[i] == "-i")
        {
            var n = int.Parse(a[i + 1]);
            if (n % 2 == 0) throw new ArgumentException("Iterations must be odd for median calculation.");
            return n;
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

// --- Formatting helpers ---

double Median(List<double> v) { v.Sort(); return v[v.Count / 2]; }
long MedianLong(List<long> v) { v.Sort(); return v[v.Count / 2]; }
int MedianInt(List<int> v) { v.Sort(); return v[v.Count / 2]; }

void CollectGarbage()
{
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
}

string FormatAllocPerMsg(long totalBytes, int count)
{
    if (count <= 0) return "n/a";
    var perMsg = (double)totalBytes / count;
    return perMsg switch
    {
        >= 1024 * 1024 => $"{perMsg / (1024 * 1024):F1} MB",
        >= 1024 => $"{perMsg / 1024:F1} KB",
        _ => $"{perMsg:F0} B"
    };
}

string FormatDiskBytes(long bytes) => bytes switch
{
    >= 1024 * 1024 * 1024 => $"{bytes / (1024.0 * 1024.0 * 1024.0):F1} GB",
    >= 1024 * 1024 => $"{bytes / (1024.0 * 1024.0):F1} MB",
    >= 1024 => $"{bytes / 1024.0:F1} KB",
    _ => $"{bytes} B"
};

// Compact one-line phase result for per-iteration output
void PrintPhaseResult(string iterLabel, string phase, RunResult r)
{
    var m = r.Metrics;
    var allocPerMsg = r.Processed > 0 ? FormatAllocPerMsg(m.AllocatedBytes, r.Processed) : "n/a";
    var d = m.Disk;
    var diskStr = d.Writes > 0
        ? $"   disk w{d.Writes,7:N0} {FormatDiskBytes(d.WriteBytes),8} await {d.AvgWriteMs,5:F1}ms busy {d.IoTimeMs,6:N0}ms"
        : "";
    Console.WriteLine(
        $"  {iterLabel,-3} {phase,-11} {r.Seconds,8:F2}s {r.Rate,10:N0}/s   " +
        $"{Dim}gc {m.Gen0,4}/{m.Gen1,4}/{m.Gen2,3}   alloc {allocPerMsg,10}   pause {m.GcPauseTime.TotalMilliseconds,7:F0}ms   cpu {m.CpuTime.TotalSeconds,6:F1}s{diskStr}{Reset}");
}

void PrintPhaseWarnings(string phase, RunResult r, int count)
{
    if (r.Errors > 0)
        Console.Error.WriteLine($"  {Yellow}    ⚠ {r.Errors:N0} errors during {phase}{Reset}");
    if (r.Processed < count)
        Console.Error.WriteLine($"  {Yellow}    ⚠ queue stalled: {phase} {r.Processed:N0}/{count:N0}{Reset}");
    if (r.Seconds < MinRunSeconds)
    {
        var recommended = (int)(r.Rate * MinRunSeconds * 1.2);
        recommended = (recommended / 10000 + 1) * 10000;
        Console.Error.WriteLine($"  {Yellow}    ⚠ Run took {r.Seconds:F2}s (<{MinRunSeconds}s) — results may be noisy. Try -n {recommended:N0}{Reset}");
    }
}

// Summary table
void PrintSummaryTable(List<SummaryRow> rows)
{
    const string sep = " | ";
    const int cScenario = 19, cPhase = 8, cMedian = 10, cAvg = 10, cAlloc = 10,
              cGc = 15, cPause = 8, cCpu = 7, cDiskW = 10, cDiskBytes = 10, cDiskAwait = 10;

    var hasDisk = rows.Any(r => r.DiskWrites > 0);

    var header = $"{sep}{"Scenario",-cScenario}{sep}{"Phase",-cPhase}{sep}{"Median/s",cMedian}{sep}{"Avg/s",cAvg}{sep}{"Alloc/msg",cAlloc}{sep}{"GC 0/1/2",cGc}{sep}{"Pause",cPause}{sep}{"CPU",cCpu}";
    var divider = $"{sep}{Dash(cScenario)}{sep}{Dash(cPhase)}{sep}{Dash(cMedian)}{sep}{Dash(cAvg)}{sep}{Dash(cAlloc)}{sep}{Dash(cGc)}{sep}{Dash(cPause)}{sep}{Dash(cCpu)}";

    if (hasDisk)
    {
        header += $"{sep}{"Disk Wr",cDiskW}{sep}{"Written",cDiskBytes}{sep}{"Wr Await",cDiskAwait}";
        divider += $"{sep}{Dash(cDiskW)}{sep}{Dash(cDiskBytes)}{sep}{Dash(cDiskAwait)}";
    }

    header += sep;
    divider += sep;

    Console.WriteLine();
    Console.WriteLine(header);
    Console.WriteLine(divider);

    string? lastScenario = null;
    foreach (var row in rows)
    {
        var scenario = row.Scenario == lastScenario ? "" : row.Scenario;
        lastScenario = row.Scenario;

        var gcStr = $"{row.Gen0,4}/{row.Gen1,4}/{row.Gen2,3}";
        var pauseStr = $"{row.PauseMs,5:F0} ms";
        var cpuStr = $"{row.CpuSec,5:F1}s";

        var line = $"{sep}{scenario,-cScenario}{sep}{row.Phase,-cPhase}{sep}" +
            $"{Bold}{Green}{row.MedianRate,cMedian:N0}{Reset}{sep}" +
            $"{row.AvgRate,cAvg:N0}{sep}" +
            $"{row.AllocPerMsg,cAlloc}{sep}" +
            $"{gcStr,cGc}{sep}" +
            $"{pauseStr,cPause}{sep}" +
            $"{cpuStr,cCpu}";

        if (hasDisk)
        {
            var awaitStr = row.DiskAwaitMs > 0 ? $"{row.DiskAwaitMs:F1} ms" : "";
            line += $"{sep}{row.DiskWrites,cDiskW:N0}{sep}{row.DiskWriteBytes,cDiskBytes}{sep}{awaitStr,cDiskAwait}";
        }

        line += sep;
        Console.WriteLine(line);
    }

    Console.WriteLine();
    return;

    static string Dash(int n) => new('-', n);
}

SummaryRow ComputeSummaryRow(string scenario, string phase, int count, List<RunResult> results)
{
    var rates = results.Select(r => r.Rate).ToList();
    var metrics = results.Select(r => r.Metrics).ToList();
    var allocPerMsg = results.Select(r => r.Processed > 0 ? r.Metrics.AllocatedBytes / r.Processed : 0L).ToList();
    var diskMetrics = metrics.Select(m => m.Disk).ToList();

    return new SummaryRow(
        scenario, phase,
        Median(new List<double>(rates)),
        rates.Average(),
        FormatAllocPerMsg(MedianLong(new List<long>(allocPerMsg)), 1),
        MedianInt(metrics.Select(m => m.Gen0).ToList()),
        MedianInt(metrics.Select(m => m.Gen1).ToList()),
        MedianInt(metrics.Select(m => m.Gen2).ToList()),
        Median(metrics.Select(m => m.GcPauseTime.TotalMilliseconds).ToList()),
        Median(metrics.Select(m => m.CpuTime.TotalSeconds).ToList()),
        MedianLong(diskMetrics.Select(d => d.Writes).ToList()),
        FormatDiskBytes(MedianLong(diskMetrics.Select(d => d.WriteBytes).ToList())),
        Median(diskMetrics.Select(d => d.AvgWriteMs).ToList())
    );
}

// --- Transport helpers ---

TransportTransactionMode MapTxMode(TxMode mode) => mode switch
{
    TxMode.None => TransportTransactionMode.None,
    TxMode.Receive => TransportTransactionMode.ReceiveOnly,
    TxMode.Atomic => TransportTransactionMode.SendsAtomicWithReceive,
    _ => TransportTransactionMode.None
};

IBMMQTransport CreateTransport(TxMode txMode)
{
    var transport = new IBMMQTransport(o =>
    {
        o.Host = ibmmqHost;
        o.Port = ibmmqPort;
        o.QueueManagerName = qmgrName;
        o.Channel = ibmmqChannel;
        o.User = ibmmqUser;
        o.Password = ibmmqPassword;
    });
    transport.TransportTransactionMode = MapTxMode(txMode);
    return transport;
}

HostSettings CreateHostSettings(bool setupInfrastructure = false) => new(
    name: "nsbbench",
    hostDisplayName: "nsbbench",
    startupDiagnostic: new StartupDiagnosticEntries(),
    criticalErrorAction: (msg, ex, _) => Console.Error.WriteLine($"Critical: {ex}"),
    setupInfrastructure: setupInfrastructure
);

OutgoingMessage CreateMessage(bool express)
{
    var id = Guid.NewGuid();
    var conversationId = Guid.NewGuid();
    var correlationId = Guid.NewGuid();
    var traceId = Convert.ToHexString(Guid.NewGuid().ToByteArray()).ToLowerInvariant();
    var spanId = Convert.ToHexString(Guid.NewGuid().ToByteArray()[..8]).ToLowerInvariant();
    var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss:ffffff") + " Z";
    var orderId = id.ToString();
    var trackingNumber = "TRACK-" + orderId[..8].ToUpperInvariant();

    var headers = new Dictionary<string, string>
    {
        [Headers.MessageId] = id.ToString(),
        [Headers.ConversationId] = conversationId.ToString(),
        [Headers.CorrelationId] = correlationId.ToString(),
        [Headers.RelatedTo] = Guid.NewGuid().ToString(),
        [Headers.ContentType] = "application/json",
        [Headers.EnclosedMessageTypes] = "Acme.OrderShipped, Acme.Shared, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null",
        [Headers.MessageIntent] = "Publish",
        [Headers.StartNewTrace] = "True",
        [Headers.OriginatingEndpoint] = "nsbbench",
        [Headers.OriginatingMachine] = Environment.MachineName,
        [Headers.ReplyToAddress] = "nsbbench",
        [Headers.TimeSent] = timestamp,
        [Headers.NServiceBusVersion] = "10.1.0",
        [Headers.DiagnosticsTraceParent] = $"00-{traceId}-{spanId}-01",
        [Headers.OriginatingHostId] = Convert.ToHexString(Guid.NewGuid().ToByteArray()).ToLowerInvariant(),
    };

    if (express)
    {
        headers[Headers.NonDurableMessage] = "true";
    }

    var body = System.Text.Encoding.UTF8.GetBytes($$"""{"OrderId":"{{orderId}}","TrackingNumber":"{{trackingNumber}}"}""");

    return new OutgoingMessage(id.ToString(), headers, body);
}

// --- Single-run helpers ---

async Task<RunResult> DoSend(string queue, TxMode txMode, bool express, int count, int concurrency)
{
    var transport = CreateTransport(txMode);
    var infrastructure = await transport.Initialize(CreateHostSettings(), [], [queue]);
    var dispatcher = infrastructure.Dispatcher;

    var remaining = count;
    const int batchSize = 1000;

    var before = MetricsSnapshot.Capture();
    var sw = Stopwatch.StartNew();

    var tasks = Enumerable.Range(0, concurrency).Select(_ => Task.Run(async () =>
    {
        while (true)
        {
            var grab = Math.Min(Interlocked.Add(ref remaining, -batchSize) + batchSize, batchSize);
            if (grab <= 0) break;

            for (var i = 0; i < grab; i++)
            {
                var msg = CreateMessage(express);
                var operation = new TransportOperation(msg, new UnicastAddressTag(queue));
                await dispatcher.Dispatch(new TransportOperations(operation), new TransportTransaction());
            }
        }
    })).ToArray();

    await Task.WhenAll(tasks);
    sw.Stop();
    var after = MetricsSnapshot.Capture();

    await infrastructure.Shutdown();

    return new RunResult(sw.Elapsed.TotalSeconds, count / sw.Elapsed.TotalSeconds, count, 0, after.Delta(before));
}

async Task<RunResult> DoReceive(string queue, TxMode txMode, int count, int concurrency)
{
    var transport = CreateTransport(txMode);
    var infrastructure = await transport.Initialize(CreateHostSettings(), [
        new ReceiveSettings("Primary", new QueueAddress(queue), usePublishSubscribe: false, purgeOnStartup: false, errorQueue: "error")
    ], []);

    var receiver = infrastructure.Receivers["Primary"];
    var received = 0;
    var errors = 0;
    var lastProgress = 0;
    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    using var stallTimer = new Timer(_ =>
    {
        var current = Volatile.Read(ref received) + Volatile.Read(ref errors);
        if (current > 0 && current == Volatile.Read(ref lastProgress))
            tcs.TrySetResult();
        Volatile.Write(ref lastProgress, current);
    }, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));

    await receiver.Initialize(
        new PushRuntimeSettings(concurrency),
        onMessage: (context, token) =>
        {
            if (Interlocked.Increment(ref received) >= count)
                tcs.TrySetResult();
            return Task.CompletedTask;
        },
        onError: (context, token) =>
        {
            Interlocked.Increment(ref errors);
            return Task.FromResult(ErrorHandleResult.Handled);
        }
    );

    var before = MetricsSnapshot.Capture();
    var sw = Stopwatch.StartNew();

    await receiver.StartReceive();
    await tcs.Task;
    sw.Stop();
    var after = MetricsSnapshot.Capture();

    await receiver.StopReceive();
    await infrastructure.Shutdown();

    var actualReceived = Volatile.Read(ref received);
    return new RunResult(sw.Elapsed.TotalSeconds, actualReceived / sw.Elapsed.TotalSeconds, actualReceived, Volatile.Read(ref errors), after.Delta(before));
}

async Task<RunResult> DoReceiveSend(string srcQueue, string destQueue, TxMode txMode, bool express, int count, int concurrency)
{
    var transport = CreateTransport(txMode);
    var infrastructure = await transport.Initialize(CreateHostSettings(), [
        new ReceiveSettings("Primary", new QueueAddress(srcQueue), usePublishSubscribe: false, purgeOnStartup: false, errorQueue: "error")
    ], [destQueue]);

    var dispatcher = infrastructure.Dispatcher;
    var receiver = infrastructure.Receivers["Primary"];
    var processed = 0;
    var errors = 0;
    var lastProgress = 0;
    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    using var stallTimer = new Timer(_ =>
    {
        var current = Volatile.Read(ref processed) + Volatile.Read(ref errors);
        if (current > 0 && current == Volatile.Read(ref lastProgress))
            tcs.TrySetResult();
        Volatile.Write(ref lastProgress, current);
    }, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));

    await receiver.Initialize(
        new PushRuntimeSettings(concurrency),
        onMessage: async (context, token) =>
        {
            var headers = new Dictionary<string, string>(context.Headers);
            if (express)
                headers[Headers.NonDurableMessage] = "true";
            else
                headers.Remove(Headers.NonDurableMessage);

            var outMsg = new OutgoingMessage(context.NativeMessageId, headers, context.Body);
            var operation = new TransportOperation(outMsg, new UnicastAddressTag(destQueue));
            await dispatcher.Dispatch(new TransportOperations(operation), context.TransportTransaction);

            if (Interlocked.Increment(ref processed) >= count)
                tcs.TrySetResult();
        },
        onError: (context, token) =>
        {
            Interlocked.Increment(ref errors);
            return Task.FromResult(ErrorHandleResult.Handled);
        }
    );

    var before = MetricsSnapshot.Capture();
    var sw = Stopwatch.StartNew();

    await receiver.StartReceive();
    await tcs.Task;
    sw.Stop();
    var after = MetricsSnapshot.Capture();

    await receiver.StopReceive();
    await infrastructure.Shutdown();

    var actualProcessed = Volatile.Read(ref processed);
    return new RunResult(sw.Elapsed.TotalSeconds, actualProcessed / sw.Elapsed.TotalSeconds, actualProcessed, Volatile.Read(ref errors), after.Delta(before));
}

// --- Commands ---

async Task<int> RunWarmup()
{
    const int warmupCount = 100;
    const string warmupQueue = "bench.warmup";

    Console.Write($"{Dim}JIT warmup...{Reset}");

    var transport = CreateTransport(TxMode.None);
    var infrastructure = await transport.Initialize(CreateHostSettings(setupInfrastructure: true), [
        new ReceiveSettings("Warmup", new QueueAddress(warmupQueue), usePublishSubscribe: false, purgeOnStartup: false, errorQueue: "error")
    ], [warmupQueue]);

    var dispatcher = infrastructure.Dispatcher;

    for (var i = 0; i < warmupCount; i++)
    {
        var msg = CreateMessage(express: true);
        var operation = new TransportOperation(msg, new UnicastAddressTag(warmupQueue));
        await dispatcher.Dispatch(new TransportOperations(operation), new TransportTransaction());
    }

    var received = 0;
    var errors = 0;
    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var receiver = infrastructure.Receivers["Warmup"];

    await receiver.Initialize(
        new PushRuntimeSettings(Environment.ProcessorCount),
        onMessage: (context, token) =>
        {
            if (Interlocked.Increment(ref received) >= warmupCount)
                tcs.TrySetResult();
            return Task.CompletedTask;
        },
        onError: (context, token) =>
        {
            if (Interlocked.Increment(ref errors) + Volatile.Read(ref received) >= warmupCount)
                tcs.TrySetResult();
            return Task.FromResult(ErrorHandleResult.Handled);
        }
    );

    await receiver.StartReceive();
    await tcs.Task;
    await receiver.StopReceive();
    await infrastructure.Shutdown();

    if (errors > 0)
        Console.Error.WriteLine($"  {Yellow}⚠ {errors:N0} errors during warmup{Reset}");
    Console.WriteLine(" done");
    return 0;
}

async Task<int> RunSend(string[] a)
{
    if (a.Length < 4) return PrintUsage();

    var queue = a[1];
    var txMode = ParseTxMode(a[2]);
    var express = ParseExpress(a[3]);
    var count = ParseCount(a, 4, express);
    var concurrency = ParseConcurrency(a, 4);
    var iterations = ParseIterations(a, 4);

    Console.WriteLine($"{Cyan}Sending {count:N0} messages (concurrency={concurrency}, tx={txMode.ToString().ToLowerInvariant()}, {(express ? "express" : "durable")}, {iterations} runs)...{Reset}");

    var results = new List<RunResult>(iterations);
    for (var iter = 0; iter < iterations; iter++)
    {
        CollectGarbage();
        var r = await DoSend(queue, txMode, express, count, concurrency);
        results.Add(r);
        PrintPhaseResult($"#{iter + 1}", "send", r);
        PrintPhaseWarnings("send", r, count);
    }

    PrintSummaryTable([ComputeSummaryRow("send", "Send", count, results)]);
    return 0;
}

async Task<int> RunReceive(string[] a)
{
    if (a.Length < 4) return PrintUsage();

    var queue = a[1];
    var txMode = ParseTxMode(a[2]);
    var express = ParseExpress(a[3]);
    var count = ParseCount(a, 4, express);
    var concurrency = ParseConcurrency(a, 4);
    var iterations = ParseIterations(a, 4);

    Console.WriteLine($"{Cyan}Receiving {count:N0} messages (concurrency={concurrency}, tx={txMode.ToString().ToLowerInvariant()}, {(express ? "express" : "durable")}, {iterations} runs)...{Reset}");

    var results = new List<RunResult>(iterations);
    for (var iter = 0; iter < iterations; iter++)
    {
        CollectGarbage();
        var r = await DoReceive(queue, txMode, count, concurrency);
        results.Add(r);
        PrintPhaseResult($"#{iter + 1}", "receive", r);
        PrintPhaseWarnings("receive", r, count);
    }

    PrintSummaryTable([ComputeSummaryRow("receive", "Receive", count, results)]);
    return 0;
}

async Task<int> RunReceiveSend(string[] a)
{
    if (a.Length < 5) return PrintUsage();

    var srcQueue = a[1];
    var destQueue = a[2];
    var txMode = ParseTxMode(a[3]);
    var express = ParseExpress(a[4]);
    var count = ParseCount(a, 5, express);
    var concurrency = ParseConcurrency(a, 5);
    var iterations = ParseIterations(a, 5);

    Console.WriteLine($"{Cyan}ReceiveSend {count:N0} messages (concurrency={concurrency}, tx={txMode.ToString().ToLowerInvariant()}, {(express ? "express" : "durable")}, {iterations} runs)...{Reset}");

    var results = new List<RunResult>(iterations);
    for (var iter = 0; iter < iterations; iter++)
    {
        CollectGarbage();
        var r = await DoReceiveSend(srcQueue, destQueue, txMode, express, count, concurrency);
        results.Add(r);
        PrintPhaseResult($"#{iter + 1}", "recvsend", r);
        PrintPhaseWarnings("recvsend", r, count);
    }

    PrintSummaryTable([ComputeSummaryRow("recvsend", "RecvSend", count, results)]);
    return 0;
}

async Task<int> RunBenchmark(string[] a)
{
    var queueA = "bench.a";
    var queueB = "bench.b";
    List<TxMode>? txModes = null;
    bool flagDurable = false, flagExpress = false;
    int? explicitCount = null;
    int concurrency = Environment.ProcessorCount;
    int iterations = 3;

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
            case "--durable":
                flagDurable = true;
                break;
            case "--express":
                flagExpress = true;
                break;
            case "-n" when i + 1 < a.Length:
                explicitCount = int.Parse(a[++i]);
                break;
            case "-c" when i + 1 < a.Length:
                concurrency = int.Parse(a[++i]);
                break;
            case "-i" when i + 1 < a.Length:
                var n = int.Parse(a[++i]);
                if (n % 2 == 0)
                    throw new ArgumentException("Iterations must be odd for median calculation.");
                iterations = n;
                break;
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

    await RunWarmup();

    Console.WriteLine($"=== nsbbench: concurrency={concurrency}, {iterations} iterations ===");
    Console.WriteLine();

    var allRows = new List<SummaryRow>();

    foreach (var express in durabilities)
    {
        foreach (var txMode in txModes)
        {
            var durLabel = express ? "express" : "durable";
            var txLabel = txMode.ToString().ToLowerInvariant();
            var scenarioLabel = $"{durLabel} tx={txLabel}";
            var count = explicitCount ?? (express ? 150_000 : 15_000);

            Console.WriteLine($"--- {scenarioLabel} ({count:N0} msgs) ---");

            var sendResults = new List<RunResult>(iterations);
            var recvSendResults = new List<RunResult>(iterations);
            var receiveResults = new List<RunResult>(iterations);

            for (var iter = 0; iter < iterations; iter++)
            {
                CollectGarbage();
                var iterLabel = $"#{iter + 1}";

                var sendResult = await DoSend(queueA, txMode, express, count, concurrency);
                sendResults.Add(sendResult);
                PrintPhaseResult(iterLabel, "send", sendResult);

                var rsResult = await DoReceiveSend(queueA, queueB, txMode, express, count, concurrency);
                recvSendResults.Add(rsResult);
                PrintPhaseResult("", "recvsend", rsResult);
                PrintPhaseWarnings("recvsend", rsResult, count);

                var recvResult = await DoReceive(queueB, txMode, count, concurrency);
                receiveResults.Add(recvResult);
                PrintPhaseResult("", "receive", recvResult);
                PrintPhaseWarnings("receive", recvResult, count);
            }

            allRows.Add(ComputeSummaryRow(scenarioLabel, "Send", count, sendResults));
            allRows.Add(ComputeSummaryRow(scenarioLabel, "RecvSend", count, recvSendResults));
            allRows.Add(ComputeSummaryRow(scenarioLabel, "Receive", count, receiveResults));

            Console.WriteLine();
        }
    }

    PrintSummaryTable(allRows);
    return 0;
}

// --- Types ---

record RunResult(double Seconds, double Rate, int Processed, int Errors, MetricsDelta Metrics);

record SummaryRow(
    string Scenario, string Phase,
    double MedianRate, double AvgRate,
    string AllocPerMsg,
    int Gen0, int Gen1, int Gen2,
    double PauseMs, double CpuSec,
    long DiskWrites, string DiskWriteBytes, double DiskAwaitMs);

record struct MetricsSnapshot(
    int Gen0, int Gen1, int Gen2,
    long AllocatedBytes,
    TimeSpan GcPauseTime,
    TimeSpan CpuTime,
    long PeakWorkingSet,
    int ThreadPoolThreads,
    int HandleCount,
    DiskSnapshot Disk)
{
    public static MetricsSnapshot Capture()
    {
        using var proc = Process.GetCurrentProcess();
        proc.Refresh();
        return new(
            GC.CollectionCount(0),
            GC.CollectionCount(1),
            GC.CollectionCount(2),
            GC.GetTotalAllocatedBytes(precise: true),
            GC.GetTotalPauseDuration(),
            proc.TotalProcessorTime,
            proc.WorkingSet64,
            ThreadPool.ThreadCount,
            proc.HandleCount,
            DiskSnapshot.Capture()
        );
    }

    public MetricsDelta Delta(MetricsSnapshot before) => new(
        Gen0 - before.Gen0,
        Gen1 - before.Gen1,
        Gen2 - before.Gen2,
        Math.Max(0, AllocatedBytes - before.AllocatedBytes),
        GcPauseTime - before.GcPauseTime,
        CpuTime - before.CpuTime,
        PeakWorkingSet,
        ThreadPoolThreads,
        HandleCount,
        Disk.Delta(before.Disk)
    );
}

record struct MetricsDelta(
    int Gen0, int Gen1, int Gen2,
    long AllocatedBytes,
    TimeSpan GcPauseTime,
    TimeSpan CpuTime,
    long PeakWorkingSet,
    int ThreadPoolThreads,
    int HandleCount,
    DiskDelta Disk);

/// <summary>
/// Reads /proc/diskstats for the device backing a given path.
/// Fields per kernel docs: https://www.kernel.org/doc/Documentation/iostats.txt
/// Sector size is 512 bytes.
/// </summary>
record struct DiskSnapshot(
    long ReadsCompleted, long SectorsRead, long ReadTimeMs,
    long WritesCompleted, long SectorsWritten, long WriteTimeMs,
    long IoTimeMs, long WeightedIoTimeMs)
{
    static readonly string? DiskDevice = DetectDiskDevice();

    static string? DetectDiskDevice()
    {
        if (!OperatingSystem.IsLinux()) return null;
        try
        {
            // Find the device for /var/lib/containers (where Podman stores MQ data)
            // by reading /proc/self/mountinfo and matching the mount point
            var target = "/var/lib/containers";
            var mountInfo = File.ReadAllLines("/proc/self/mountinfo");
            string? majorMinor = null;
            int bestLen = 0;
            foreach (var line in mountInfo)
            {
                var parts = line.Split(' ');
                if (parts.Length < 5) continue;
                var mountPoint = parts[4];
                // Find longest prefix match
                if (target.StartsWith(mountPoint) && mountPoint.Length > bestLen)
                {
                    majorMinor = parts[2]; // "major:minor"
                    bestLen = mountPoint.Length;
                }
            }

            if (majorMinor == null) return null;

            // Find the whole-disk device (minor 0 or the parent device)
            var mmParts = majorMinor.Split(':');
            var major = mmParts[0];

            // Read /proc/diskstats to find the whole-disk device with this major number
            foreach (var line in File.ReadAllLines("/proc/diskstats"))
            {
                var fields = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (fields.Length >= 3 && fields[0] == major && fields[1] == "0")
                    return fields[2]; // whole disk (minor 0)
            }

            // Fallback: use the partition's parent by stripping trailing digits/p-digits
            foreach (var line in File.ReadAllLines("/proc/diskstats"))
            {
                var fields = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (fields.Length >= 3 && fields[0] == major)
                {
                    var name = fields[2];
                    // NVMe: nvme0n1p3 → nvme0n1, SCSI: sda1 → sda
                    var parent = name.Contains("nvme")
                        ? name[..name.LastIndexOf('p')]
                        : name.TrimEnd("0123456789".ToCharArray());
                    return parent;
                }
            }
        }
        catch { /* not on Linux or /proc unavailable */ }
        return null;
    }

    public static DiskSnapshot Capture()
    {
        if (DiskDevice == null) return default;
        try
        {
            foreach (var line in File.ReadAllLines("/proc/diskstats"))
            {
                var f = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (f.Length >= 14 && f[2] == DiskDevice)
                {
                    return new(
                        long.Parse(f[3]),  // reads completed
                        long.Parse(f[5]),  // sectors read
                        long.Parse(f[6]),  // read time ms
                        long.Parse(f[7]),  // writes completed
                        long.Parse(f[9]),  // sectors written
                        long.Parse(f[10]), // write time ms
                        long.Parse(f[12]), // io time ms
                        long.Parse(f[13])  // weighted io time ms
                    );
                }
            }
        }
        catch { /* ignore */ }
        return default;
    }

    public DiskDelta Delta(DiskSnapshot before)
    {
        var reads = ReadsCompleted - before.ReadsCompleted;
        var writes = WritesCompleted - before.WritesCompleted;
        var readBytes = (SectorsRead - before.SectorsRead) * 512;
        var writeBytes = (SectorsWritten - before.SectorsWritten) * 512;
        var writeTimeMs = WriteTimeMs - before.WriteTimeMs;
        var ioTimeMs = IoTimeMs - before.IoTimeMs;
        var avgWriteMs = writes > 0 ? (double)writeTimeMs / writes : 0;

        return new(reads, writes, readBytes, writeBytes, avgWriteMs, ioTimeMs);
    }
}

record struct DiskDelta(
    long Reads, long Writes,
    long ReadBytes, long WriteBytes,
    double AvgWriteMs, long IoTimeMs);

enum TxMode { None, Receive, Atomic }

class StderrLoggerFactory : ILoggerFactory
{
    static readonly StreamWriter Writer = new(Console.OpenStandardError(), leaveOpen: true) { AutoFlush = true };

    public ILog GetLogger(Type type) => new StderrLogger(Writer);
    public ILog GetLogger(string name) => new StderrLogger(Writer);
}

class StderrLogger(StreamWriter writer) : ILog
{
    public bool IsDebugEnabled => false;
    public bool IsInfoEnabled => false;
    public bool IsWarnEnabled => true;
    public bool IsErrorEnabled => true;
    public bool IsFatalEnabled => true;

    public void Debug(string? message) { }
    public void Debug(string? message, Exception? exception) { }
    public void DebugFormat(string format, params object?[] args) { }
    public void Info(string? message) { }
    public void Info(string? message, Exception? exception) { }
    public void InfoFormat(string format, params object?[] args) { }

    public void Warn(string? message) => writer.WriteLine($"WARN: {message}");
    public void Warn(string? message, Exception? exception) => writer.WriteLine($"WARN: {message} {exception}");
    public void WarnFormat(string format, params object?[] args) => writer.WriteLine($"WARN: {string.Format(format, args)}");

    public void Error(string? message) => writer.WriteLine($"ERROR: {message}");
    public void Error(string? message, Exception? exception) => writer.WriteLine($"ERROR: {message} {exception}");
    public void ErrorFormat(string format, params object?[] args) => writer.WriteLine($"ERROR: {string.Format(format, args)}");

    public void Fatal(string? message) => writer.WriteLine($"FATAL: {message}");
    public void Fatal(string? message, Exception? exception) => writer.WriteLine($"FATAL: {message} {exception}");
    public void FatalFormat(string format, params object?[] args) => writer.WriteLine($"FATAL: {string.Format(format, args)}");
}
