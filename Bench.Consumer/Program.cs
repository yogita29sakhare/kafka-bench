// See https://aka.ms/new-console-template for more information
// Bench.Consumer/Program.cs
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using System.Diagnostics;

class Program
{
    static int Main(string[] args)
    {
        string brokerType = GetArg(args, "--broker") ?? "kafka";
        
        string bootstrap = brokerType.ToLower() switch
        {
            "redpanda" => GetArg(args, "--bootstrap") ?? "localhost:19092",
            _ => GetArg(args, "--bootstrap") ?? "localhost:9092"
        };
        string topic = GetArg(args, "--topic") ?? "bench-topic";
        int expected = int.Parse(GetArg(args, "--total") ?? "0"); // 0 means indefinite/continuous
        string groupId = GetArg(args, "--group") ?? $"bench-consumer-{Guid.NewGuid():N}";

        Console.WriteLine($"Consumer: bootstrap={bootstrap}, topic={topic}, group={groupId}, expected={expected}");

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        var e2eLatencies = new List<double>();
        int consumed = 0;

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        var cts = new CancellationTokenSource();

        try
        {
            while (!cts.IsCancellationRequested)
            {
                var cr = consumer.Consume(TimeSpan.FromSeconds(1));
                if (cr == null) continue;

                // compute end-to-end if header exists
                if (cr.Message.Headers.TryGetLastBytes("x-ts-ms", out var headerBytes) && headerBytes.Length == 8)
                {
                    long producedMs = BitConverter.ToInt64(headerBytes, 0);
                    long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    double latency = nowMs - producedMs;
                    e2eLatencies.Add(latency);
                }

                consumed++;
                if (expected > 0 && consumed >= expected)
                {
                    Console.WriteLine("Reached expected count, exiting.");
                    break;
                }

                if (consumed % 10000 == 0)
                {
                    Console.WriteLine($"Consumed {consumed} messages...");
                }
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            consumer.Close();
        }

        // compute p95
        if (e2eLatencies.Count > 0)
        {
            var arr = e2eLatencies.ToArray();
            Array.Sort(arr);
            double p95 = arr[(int)Math.Ceiling(0.95 * arr.Length) - 1];
            Console.WriteLine($"Consumed {consumed} messages; E2E samples={arr.Length}; p95_ms={p95:F2}");
        }
        else
        {
            Console.WriteLine($"Consumed {consumed} messages; no E2E header samples found.");
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
}

