using BenchmarkDotNet.Running;
using B3.Umdf.FixConflated.Benchmarks;

BenchmarkSwitcher.FromAssembly(typeof(FixApplicationMessageWriterBenchmarks).Assembly).Run(args);
