using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BytexDigital.Steam.ContentDelivery.Models;
using BytexDigital.Steam.ContentDelivery.Models.Downloading;
using Xunit;

namespace BytexDigital.Steam.Tests.ContentDelivery.Models.Downloading;

public class MultiDepotDownloadHandlerTests
{
    [Fact]
    public async Task EmptyHandlerList_ReportsCompletedProgressAndZeroSize()
    {
        var handler = new MultiDepotDownloadHandler(
            new List<IDownloadHandler>());

        Assert.Equal(1d, handler.TotalProgress);
        Assert.Equal(0, handler.TotalFileCount);
        Assert.Equal(0UL, handler.TotalFileSize);

        await handler.SetupAsync("test", _ => true);
        await handler.VerifyAsync();
        await handler.DownloadAsync();

        Assert.Equal(
            DownloadHandlerStateEnum.Downloaded,
            handler.State);
    }

    [Fact]
    public async Task DownloadAsync_Success_SetsDownloadedState()
    {
        var child = new FakeDownloadHandler();
        var handler = await CreateVerifiedHandlerAsync(child);
        var completionCount = 0;

        handler.DownloadComplete += (_, _) => completionCount++;

        await handler.DownloadAsync();

        Assert.Equal(
            DownloadHandlerStateEnum.Downloaded,
            handler.State);
        Assert.Equal(
            DownloadHandlerStateEnum.Downloaded,
            child.State);
        Assert.Equal(1, completionCount);
    }

    [Fact]
    public async Task VerifyAsync_Failure_PropagatesExceptionAndSetsFailedState()
    {
        var child = new FakeDownloadHandler(
            verificationException: new InvalidOperationException(
                "Verification failed."));

        var handler = new MultiDepotDownloadHandler(
            new List<IDownloadHandler> { child });

        await handler.SetupAsync("test", _ => true);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => handler.VerifyAsync());

        Assert.Equal(
            DownloadHandlerStateEnum.Failed,
            handler.State);
    }

    [Fact]
    public async Task DownloadAsync_Failure_PropagatesExceptionAndSetsFailedState()
    {
        var child = new FakeDownloadHandler(
            downloadException: new InvalidOperationException(
                "Download failed."));

        var handler = await CreateVerifiedHandlerAsync(child);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => handler.DownloadAsync());

        Assert.Equal(
            DownloadHandlerStateEnum.Failed,
            handler.State);
    }

    [Fact]
    public async Task DownloadAsync_Cancellation_PropagatesAndSetsFailedState()
    {
        var child = new FakeDownloadHandler();
        var handler = await CreateVerifiedHandlerAsync(child);

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => handler.DownloadAsync(cancellation.Token));

        Assert.Equal(
            DownloadHandlerStateEnum.Failed,
            handler.State);
    }

    private static async Task<MultiDepotDownloadHandler>
        CreateVerifiedHandlerAsync(IDownloadHandler child)
    {
        var handler = new MultiDepotDownloadHandler(
            new List<IDownloadHandler> { child });

        await handler.SetupAsync("test", _ => true);
        await handler.VerifyAsync();

        return handler;
    }

    private sealed class FakeDownloadHandler : IDownloadHandler
    {
        private readonly Exception? _verificationException;
        private readonly Exception? _downloadException;

        public FakeDownloadHandler(
            Exception? verificationException = null,
            Exception? downloadException = null,
            double progress = 0.5,
            ulong size = 100UL)
        {
            _verificationException = verificationException;
            _downloadException = downloadException;
            TotalProgress = progress;
            TotalFileSize = size;
        }

        public double TotalProgress { get; }
        public int TotalFileCount => 1;
        public ulong TotalFileSize { get; }

        public string DownloadDirectory { get; private set; } = string.Empty;

        public Func<ManifestFile, bool> FileCondition { get; private set; }
            = _ => true;

        public DownloadHandlerStateEnum State { get; private set; }
            = DownloadHandlerStateEnum.Created;

        public event EventHandler<FileVerifiedArgs> FileVerified
        {
            add { }
            remove { }
        }

        public event EventHandler<VerificationCompletedArgs> VerificationCompleted
        {
            add { }
            remove { }
        }

        public event EventHandler<ManifestFile> FileDownloaded
        {
            add { }
            remove { }
        }

        public event EventHandler<EventArgs> DownloadComplete
        {
            add { }
            remove { }
        }

        public Task SetupAsync(
            string directory,
            Func<ManifestFile, bool> condition,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            DownloadDirectory = directory;
            FileCondition = condition;
            State = DownloadHandlerStateEnum.SetUp;

            return Task.CompletedTask;
        }

        public Task VerifyAsync(
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_verificationException is not null)
            {
                throw _verificationException;
            }

            State = DownloadHandlerStateEnum.Verified;
            return Task.CompletedTask;
        }

        public Task DownloadAsync(
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_downloadException is not null)
            {
                throw _downloadException;
            }

            State = DownloadHandlerStateEnum.Downloaded;
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public void Dispose()
        {
        }
    }

    [Fact]
    public void TotalProgress_IsWeightedBySize()
    {
        var small = new FakeDownloadHandler(progress: 1.0, size: 10UL);
        var large = new FakeDownloadHandler(progress: 0.0, size: 90UL);

        var handler = new MultiDepotDownloadHandler(
            new List<IDownloadHandler> { small, large });

        // A plain average would say 0.5
        Assert.Equal(0.1, handler.TotalProgress, 3);
    }

    [Fact]
    public void TotalProgress_IgnoresDepotsWithNothingToDownload()
    {
        var nothing = new FakeDownloadHandler(progress: 0.0, size: 0UL);
        var done = new FakeDownloadHandler(progress: 1.0, size: 100UL);

        var handler = new MultiDepotDownloadHandler(
            new List<IDownloadHandler> { nothing, done });

        Assert.Equal(1.0, handler.TotalProgress, 3);
    }
}