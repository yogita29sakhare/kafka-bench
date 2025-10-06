// See https://aka.ms/new-console-template for more information
// Bench.Producer/Program.cs
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program
{
    static async Task<int> Main(string[] args)
    {
        // ---------- Simple arg parsing ----------
        string brokerType = GetArg(args, "--broker") ?? "kafka"; // kafka|redpanda
                                                                
        string bootstrap = brokerType.ToLower() switch
        {
            "redpanda" => GetArg(args, "--bootstrap") ?? "localhost:19092",
            _ => GetArg(args, "--bootstrap") ?? "localhost:9092"
        };

        string topic = GetArg(args, "--topic") ?? "bench-topic";
        int concurrency = int.Parse(GetArg(args, "--concurrency") ?? "4");
        int total = int.Parse(GetArg(args, "--total") ?? "100000");
        int seed = int.Parse(GetArg(args, "--seed") ?? "12345");
        string csvOut = GetArg(args, "--csv") ?? "benchmark_summary.csv";  // optional filename

        Console.WriteLine($"Broker: {brokerType}, Bootstrap: {bootstrap}, Topic: {topic}, Concurrency: {concurrency}, Total: {total}, Seed: {seed}, CSVOut : {csvOut}");

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = Acks.All,            // same for both brokers
            LingerMs = 5,
            BatchSize = 65536,
            MessageTimeoutMs = 300000,
            MaxInFlight = 5,
            // Compression = CompressionType.Lz4 // optional
        };

        // Precompute deterministic type sequence (so concurrency does not change mix)
        string[] types = GenerateTypeSequence(total, seed);

        var latencies = new ConcurrentBag<double>(); // ms
        var swTotal = Stopwatch.StartNew();
        int nextIndex = 0;

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var tasks = Enumerable.Range(0, concurrency)
            .Select(_ => Task.Run(async () =>
            {
                while (true)
                {
                    int idx = Interlocked.Increment(ref nextIndex);
                    if (idx > total) break;

                    string msgType = types[idx - 1];
                    string payload = GeneratePayload(msgType, idx, seed + idx, targetBytes: 512);
                    var headers = new Headers();
                    long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    headers.Add("x-ts-ms", BitConverter.GetBytes(nowMs));

                    var message = new Message<string, string>
                    {
                        Key = msgType + "-" + (idx % 1000),
                        Value = payload,
                        Headers = headers
                    };

                    long t0 = Stopwatch.GetTimestamp();
                    // ProduceAsync returns when the broker acknowledges (delivery report)
                    try
                    {
                        var dr = await producer.ProduceAsync(topic, message).ConfigureAwait(false);
                    }
                    catch (ProduceException<string, string> pex)
                    {
                        Console.Error.WriteLine($"Produce error: {pex.Error.Reason}");
                        continue;
                    }
                    long t1 = Stopwatch.GetTimestamp();
                    double ms = (t1 - t0) * 1000.0 / Stopwatch.Frequency;
                    latencies.Add(ms);
                }
            }))
            .ToArray();

        await Task.WhenAll(tasks);
        // wait for outstanding
        producer.Flush(TimeSpan.FromSeconds(30));
        swTotal.Stop();

        var latArr = latencies.ToArray();
        Array.Sort(latArr);
        double p50 = PercentileNearestRank(latArr, 0.50);
        double p95 = PercentileNearestRank(latArr, 0.95);
        double throughput = total / swTotal.Elapsed.TotalSeconds;

        // Print summary table
        Console.WriteLine();
        Console.WriteLine("---- Summary ----");
        Console.WriteLine($"broker,concurrency,total,throughput(msgs/s),p50_ms,p95_ms");
        Console.WriteLine($"{brokerType},{concurrency},{total},{throughput:F1},{p50:F2},{p95:F2}");

        if (!string.IsNullOrEmpty(csvOut))
        {
            var csv = new StringBuilder();
            csv.AppendLine("broker,concurrency,total,throughput,p50_ms,p95_ms");
            csv.AppendLine($"{brokerType},{concurrency},{total},{throughput:F1},{p50:F2},{p95:F2}");
            System.IO.File.WriteAllText(csvOut, csv.ToString());
            Console.WriteLine($"Wrote CSV to {csvOut}");
        }

        return 0;
    }

    static string GetArg(string[] args, string name)
    {
        for (int i = 0; i < args.Length; i++)
            if (args[i].Equals(name, StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
                return args[i + 1];
        return null;
    }

    // Deterministic sequence with 70/20/10 proportion
    static string[] GenerateTypeSequence(int total, int seed)
    {
        var r = new Random(seed);
        var arr = new string[total];
        for (int i = 0; i < total; i++)
        {
            double v = r.NextDouble();
            if (v < 0.70) arr[i] = "OrderPlaced";
            else if (v < 0.90) arr[i] = "PaymentSettled";
            else arr[i] = "InventoryAdjusted";
        }
        return arr;
    }

    // Build JSON payload ~targetBytes in UTF-8
    static string GeneratePayload(string msgType, int seq, int padSeed, int targetBytes = 512)
    {
        var obj = new
        {
            id = Guid.NewGuid().ToString(),
            type = msgType,
            sequence = seq,
            ts = DateTimeOffset.UtcNow.ToString("o"),
            data = new { amount = (seq % 1000) + 0.5, items = seq % 5 }
        };

        string json = JsonSerializer.Serialize(obj);
        int curLen = Encoding.UTF8.GetByteCount(json);
        int padNeeded = targetBytes - curLen - 20; // leave some margin for JSON overhead
        if (padNeeded > 0)
        {
            var rnd = new Random(padSeed);
            var sb = new StringBuilder();
            while (sb.Length < padNeeded)
            {
                // deterministic pseudo-random text per message
                int chunk = Math.Min(padNeeded - sb.Length, 32);
                for (int i = 0; i < chunk; i++)
                    sb.Append((char)('a' + rnd.Next(0, 26)));
            }
            var withPad = new { id = obj.id, type = obj.type, sequence = obj.sequence, ts = obj.ts, data = obj.data, pad = sb.ToString() };
            return JsonSerializer.Serialize(withPad);
        }
        return json;
    }

    static double PercentileNearestRank(double[] sorted, double p)
    {
        if (sorted == null || sorted.Length == 0) return 0;
        int n = sorted.Length;
        int idx = Math.Max(0, (int)Math.Ceiling(p * n) - 1);
        return sorted[idx];
    }
}

