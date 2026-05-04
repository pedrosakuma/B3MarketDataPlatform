using System;
using System.Collections.Generic;
using System.Text;

namespace B3.Umdf.Fuzz.Tests;

/// <summary>
/// Tiny hand-rolled property runner. We deliberately avoid pulling FsCheck for
/// this POC (see report.md): the parsers we want to fuzz expose ref-struct
/// callbacks (<c>in TData</c>, <see cref="ReadOnlySpan{T}"/>) that don't compose
/// cleanly with FsCheck arbitraries, and we want the suite to stay deterministic
/// (seedable) and fast (sub-second) so CI failures are reproducible.
/// </summary>
internal static class PropertyRunner
{
    public const int DefaultSeed = 0xB35BE;

    /// <summary>
    /// Generates <paramref name="iterations"/> random <c>byte[]</c> samples of
    /// length in [0, <paramref name="maxLength"/>] and feeds each one to
    /// <paramref name="property"/>. On the first failure we throw an Xunit
    /// assertion that includes the offending input as a hex string AND the
    /// seed/iteration index — copy-paste enough for the dev to reproduce.
    /// </summary>
    public static void ForAllBytes(
        int iterations,
        int maxLength,
        Action<byte[]> property,
        int seed = DefaultSeed)
    {
        var rng = new Random(seed);
        for (int i = 0; i < iterations; i++)
        {
            int len = rng.Next(0, maxLength + 1);
            var buf = new byte[len];
            rng.NextBytes(buf);

            try
            {
                property(buf);
            }
            catch (Xunit.Sdk.XunitException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Assert.Fail(
                    $"Property failed at iteration {i} (seed={seed}, length={len}).\n" +
                    $"Input (hex): {ToHex(buf)}\n" +
                    $"Exception: {ex.GetType().FullName}: {ex.Message}\n" +
                    $"{ex.StackTrace}");
            }
        }
    }

    /// <summary>
    /// Generates sequences of commands and applies them to a fresh model.
    /// After each command the <paramref name="invariant"/> is re-checked. On
    /// failure we report the full command trace plus the seed so the failure
    /// can be replayed deterministically.
    /// </summary>
    public static void ForAllCommandSequences<TModel, TCommand>(
        int iterations,
        int maxCommandsPerRun,
        Func<TModel> modelFactory,
        Func<Random, TCommand> commandFactory,
        Action<TModel, TCommand> apply,
        Action<TModel> invariant,
        int seed = DefaultSeed)
    {
        var rng = new Random(seed);
        for (int i = 0; i < iterations; i++)
        {
            int n = rng.Next(1, maxCommandsPerRun + 1);
            var model = modelFactory();
            var trace = new List<TCommand>(n);
            for (int j = 0; j < n; j++)
            {
                var cmd = commandFactory(rng);
                trace.Add(cmd);
                try
                {
                    apply(model, cmd);
                    invariant(model);
                }
                catch (Xunit.Sdk.XunitException ex)
                {
                    throw new Xunit.Sdk.XunitException(
                        $"Invariant violated at iteration {i}, command {j + 1}/{n} (seed={seed}).\n" +
                        $"Trace: [{string.Join(", ", trace)}]\n" +
                        $"Original: {ex.Message}");
                }
                catch (Exception ex)
                {
                    Assert.Fail(
                        $"Unexpected exception at iteration {i}, command {j + 1}/{n} (seed={seed}).\n" +
                        $"Trace: [{string.Join(", ", trace)}]\n" +
                        $"Exception: {ex.GetType().FullName}: {ex.Message}\n" +
                        $"{ex.StackTrace}");
                }
            }
        }
    }

    private static string ToHex(byte[] data)
    {
        if (data.Length == 0) return "<empty>";
        var sb = new StringBuilder(data.Length * 2);
        foreach (var b in data) sb.Append(b.ToString("x2"));
        return sb.ToString();
    }
}
