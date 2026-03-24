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

if (args.Length < 3)
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
    _ => PrintUsage()
};

int PrintUsage()
{
    Console.Error.WriteLine("Usage:");
    Console.Error.WriteLine("  mqbench send <queue> <txmode> <durability> [-n <count>] [-c <concurrency>] [-i <iterations>]");
    Console.Error.WriteLine("  mqbench receive <queue> <txmode> <durability> [-n <count>] [-c <concurrency>] [-i <iterations>]");
    Console.Error.WriteLine("  mqbench receivesend <srcqueue> <destqueue> <txmode> <durability> [-n <count>] [-c <concurrency>] [-i <iterations>]");
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

enum TxMode { None, Receive, Atomic }
