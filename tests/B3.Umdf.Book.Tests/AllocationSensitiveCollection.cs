namespace B3.Umdf.Book.Tests;

/// <summary>
/// xUnit test collection that disables parallel execution. Tests that measure
/// per-thread allocations (via <c>GC.GetAllocatedBytesForCurrentThread</c>)
/// must not run alongside other tests on the same thread, otherwise other
/// tests' allocations contaminate the measured delta. Membership:
///
///   - <see cref="BookManagerOnPacketAllocationTests"/> (P11-1 closure regression).
/// </summary>
[CollectionDefinition(nameof(AllocationSensitiveCollection), DisableParallelization = true)]
public sealed class AllocationSensitiveCollection
{
}
