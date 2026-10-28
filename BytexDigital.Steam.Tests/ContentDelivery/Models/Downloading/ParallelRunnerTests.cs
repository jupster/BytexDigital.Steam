using BytexDigital.Steam.ContentDelivery.Models.Downloading;

namespace BytexDigital.Steam.Tests.ContentDelivery.Models.Downloading;

public class ParallelRunnerTests
{
    [Fact]
    public async Task RunsEverythingAndNeverExceedsTheLimit()
    {
        var running = 0;
        var peak = 0;
        var done = 0;

        var factories = Enumerable.Range(0, 20)
            .Select(_ => new Func<CancellationToken, Task>(async ct =>
            {
                var current = Interlocked.Increment(ref running);
                int seen;
                do
                {
                    seen = Volatile.Read(ref peak);
                }
                while (current > seen && Interlocked.CompareExchange(ref peak, current, seen) != seen);

                await Task.Delay(10, ct);

                Interlocked.Decrement(ref running);
                Interlocked.Increment(ref done);
            }))
            .ToList();

        await ParallelRunner.RunAsync(3, factories, default);

        Assert.Equal(20, done);
        Assert.True(peak <= 3, $"Peak parallelism was {peak}");
    }

    [Fact]
    public async Task OneFailureCancelsTheOthersAndWaitsForThem()
    {
        var stillRunning = 0;

        Func<CancellationToken, Task> waitsForever = async ct =>
        {
            Interlocked.Increment(ref stillRunning);
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            finally
            {
                Interlocked.Decrement(ref stillRunning);
            }
        };

        var factories = new List<Func<CancellationToken, Task>>
        {
            waitsForever,
            waitsForever,
            async ct =>
            {
                await Task.Delay(20, CancellationToken.None);
                throw new InvalidOperationException("boom");
            }
        };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ParallelRunner.RunAsync(3, factories, default));

        // RunAsync only returns once the others have stopped
        Assert.Equal(0, stillRunning);
    }

    [Fact]
    public async Task CancellationStopsTheRunAndThrows()
    {
        using var cancellation = new CancellationTokenSource();
        var started = 0;

        var factories = Enumerable.Range(0, 10)
            .Select(_ => new Func<CancellationToken, Task>(async ct =>
            {
                Interlocked.Increment(ref started);
                await Task.Delay(Timeout.Infinite, ct);
            }))
            .ToList();

        var run = ParallelRunner.RunAsync(2, factories, cancellation.Token);
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
        Assert.Equal(2, started);
    }
}