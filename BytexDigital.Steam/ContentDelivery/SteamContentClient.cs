using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics.CodeAnalysis;
using BytexDigital.Steam.ContentDelivery.Exceptions;
using BytexDigital.Steam.ContentDelivery.Models;
using BytexDigital.Steam.ContentDelivery.Models.Downloading;
using BytexDigital.Steam.Core;
using BytexDigital.Steam.Core.Enumerations;
using BytexDigital.Steam.Core.Structs;
using Nito.AsyncEx;
using SteamKit2.CDN;
using SteamKit2.Internal;
using SteamKit = SteamKit2;

namespace BytexDigital.Steam.ContentDelivery;

public class SteamContentClient : IAsyncDisposable
{
    // Cache
    private readonly ConcurrentDictionary<AppId, ulong> _appAccessTokens = new();

    // Cache
    private readonly ConcurrentDictionary<uint, SteamKit.SteamApps.PICSProductInfoCallback.PICSProductInfo>
        _appProductInfos = new();

    private readonly CancellationTokenSource _cancellationTokenSource;

    // Cache
    private readonly ConcurrentDictionary<DepotId, byte[]> _depotKeys = new();

    // Cache
    private readonly ConcurrentDictionary<uint, ulong> _packageAccessTokens = new();

    // Cache
    private readonly ConcurrentDictionary<uint, SteamKit.SteamApps.PICSProductInfoCallback.PICSProductInfo>
        _packageProductInfos = new();

    // Shared server pools
    private readonly ConcurrentDictionary<AppId, SteamCdnServerPool> _serverPools = new();

    private readonly AsyncLock _serverPoolsLock = new();

    public CancellationToken CancellationToken => _cancellationTokenSource.Token;
    public SteamClient SteamClient { get; }

    internal SteamKit.Internal.PublishedFile PublishedFileService { get; }
    internal SteamKit.SteamUser SteamUser { get; }
    internal SteamKit.SteamApps SteamApps { get; }
    internal SteamKit.SteamUnifiedMessages SteamUnifiedMessagesService { get; }
    internal int MaxConcurrentDownloadsPerTask { get; }

    public SteamContentClient(SteamClient steamClient, int maxConcurrentDownloadsPerTask = 10)
    {
        SteamClient = steamClient;
        SteamUnifiedMessagesService = SteamClient.InternalClient.GetHandler<SteamKit.SteamUnifiedMessages>();
        PublishedFileService = SteamUnifiedMessagesService.CreateService<PublishedFile>();
        MaxConcurrentDownloadsPerTask = maxConcurrentDownloadsPerTask;
        SteamApps = SteamClient.InternalClient.GetHandler<SteamKit.SteamApps>();
        SteamUser = SteamClient.InternalClient.GetHandler<SteamKit.SteamUser>();

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(SteamClient.CancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        foreach (var pool in _serverPools.Values)
        {
            pool.Dispose();
        }

        _serverPools.Clear();

        _cancellationTokenSource.Cancel();
        _cancellationTokenSource.Dispose();

        return ValueTask.CompletedTask;
    }

    public async Task<Manifest> GetManifestAsync(
        AppId appId,
        DepotId depotId,
        ManifestId manifestId,
        CancellationToken cancellationToken = default)
    {
        await SteamClient.AwaitReadyAsync(cancellationToken);

        var pool = await GetSteamCdnServerPoolAsync(appId, cancellationToken);

        if (manifestId == default)
        {
            throw new SteamManifestDownloadException($"Manifest with id = {manifestId} cannot be downloaded.");
        }

        var attempts = 0;
        const int maxAttempts = 5;

        Exception lastException = default;

        while (attempts < maxAttempts && !cancellationToken.IsCancellationRequested)
        {
            cancellationToken.ThrowIfCancellationRequested();

            Server server = default;
            var healthy = false;

            try
            {
                server = pool.GetServer(cancellationToken);

                var depotKey = await GetDepotKeyAsync(depotId, appId);

                var manifestCode =
                    await SteamClient._steamContentHandler.GetManifestRequestCode(depotId, appId, manifestId)
                        .WaitAsync(cancellationToken);

                var manifest =
                    await pool.CdnClient.DownloadManifestAsync(depotId, manifestId, manifestCode, server, depotKey)
                        .WaitAsync(cancellationToken);

                if (manifest.FilenamesEncrypted)
                {
                    manifest.DecryptFilenames(depotKey);
                }

                healthy = true;
                return manifest;
            }
            catch (Exception ex) when (ex is SteamAccessDeniedException or SteamNoContentServerFoundException)
            {
                throw;
            }
            catch (Exception ex)
            {
                lastException = ex;
            }
            finally
            {
                // A server that just worked goes back in the pool. Only one that failed is dropped.
                if (server != null)
                {
                    pool.ReturnServer(server, !healthy);
                }

                attempts += 1;
            }
        }

        cancellationToken.ThrowIfCancellationRequested();

        throw new SteamManifestDownloadException(
            $"Could not download the manifest = {manifestId} after {maxAttempts} attempts.",
            lastException);
    }

    private async Task<SteamCdnServerPool> GetSteamCdnServerPoolAsync(
        AppId appId,
        CancellationToken cancellationToken = default)
    {
        using var lk = await _serverPoolsLock.LockAsync(cancellationToken);

        if (_serverPools.TryGetValue(appId, out var serverPool))
        {
            if (!serverPool.IsExhausted)
            {
                return serverPool;
            }

            // The pool gave up because Steam returned no servers. Replace it so later calls can try again.
            serverPool.Dispose();
        }

        serverPool = new SteamCdnServerPool(this, appId);
        _serverPools[appId] = serverPool;

        return serverPool;
    }

    public async Task<byte[]> GetDepotKeyAsync(DepotId depotId, AppId appId)
    {
        if (_depotKeys.TryGetValue(depotId, out var cachedKey))
        {
            return cachedKey;
        }

        var result = await SteamApps.GetDepotDecryptionKey(depotId, appId);

        if (result.Result != SteamKit.EResult.OK)
        {
            if (result.Result == SteamKit.EResult.AccessDenied)
            {
                throw new SteamDepotAccessDeniedException(depotId);
            }

            throw new SteamDepotKeyNotRetrievableException(depotId, result.Result);
        }

        // If another thread stored a key while we were waiting on Steam, keep theirs
        return _depotKeys.GetOrAdd(depotId, result.DepotKey);
    }

    public async Task<PublishedFileDetails> GetPublishedFileDetailsAsync(
        PublishedFileId publishedFileId,
        CancellationToken cancellationToken = default)
    {
        var data = (await GetPublishedFileDetailsAsync(new[] { publishedFileId }, cancellationToken))
            .FirstOrDefault();

        if (data == null)
        {
            throw new SteamPublishedFileDetailsFetchException();
        }

        return data;
    }

    public async Task<IReadOnlyList<PublishedFileDetails>> GetPublishedFileDetailsAsync(
        IEnumerable<PublishedFileId> publishedFileIds,
        CancellationToken cancellationToken = default)
    {
        var request = new CPublishedFile_GetDetails_Request();

        foreach (var publishedFileId in publishedFileIds)
        {
            request.publishedfileids.Add(publishedFileId);
        }

        var result = await PublishedFileService.GetDetails(request)
            .ToTask()
            .WaitAsync(cancellationToken);

        if (result.Result != SteamKit.EResult.OK)
        {
            throw new SteamPublishedFileDetailsFetchException(result.Result);
        }

        return result.Body.publishedfiledetails;
    }

    public async Task<IList<PublishedFileDetails>> GetPublishedFilesForAppIdRawAsync(
        AppId appId,
        CancellationToken cancellationToken = default)
    {
        var paginationCursor = "*";
        var items = new List<PublishedFileDetails>();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var requestDetails = new CPublishedFile_QueryFiles_Request
            {
                query_type = (uint) SteamKit.EPublishedFileQueryType.RankedByPublicationDate,
                cursor = paginationCursor,
                creator_appid = appId,
                appid = appId,
                numperpage = 100,

                return_vote_data = true,
                return_children = true,
                return_for_sale_data = true,
                return_kv_tags = true,
                return_metadata = true,
                return_tags = true,
                return_previews = true,
                return_details = true,
                return_short_description = true
            };


            var methodResponse = await PublishedFileService.QueryFiles(requestDetails)
                .ToTask()
                .WaitAsync(cancellationToken);

            var returnedItems = methodResponse.Body;

            items.AddRange(returnedItems.publishedfiledetails);
            paginationCursor = returnedItems.next_cursor;

            if (returnedItems.publishedfiledetails.Count == 0)
            {
                break;
            }
        }

        return items;
    }

    public async Task<IDownloadHandler> GetPublishedFileDataAsync(
        PublishedFileId publishedFileId,
        ManifestId? manifestId = default,
        CancellationToken cancellationToken = default)
    {
        var publishedFileDetails = await GetPublishedFileDetailsAsync(publishedFileId, cancellationToken);

        if (publishedFileDetails.consumer_appid == 0)
        {
            throw new SteamPublishedFileNotFoundException(publishedFileId);
        }

        if (publishedFileDetails.file_type != (uint) SteamKit.EWorkshopFileType.Community)
        {
            throw new SteamPublishedFileIsNotDownloadableException(
                publishedFileId,
                (SteamKit.EWorkshopFileType) publishedFileDetails.file_type);
        }

        if (!string.IsNullOrEmpty(publishedFileDetails.file_url))
        {
            return new LegacyDownloadHandler(publishedFileDetails.file_url, publishedFileDetails.filename);
        }

        // Request access if necessary
        if (!await GetHasAccessAsync(publishedFileDetails.consumer_appid, cancellationToken))
        {
            var gotFreeLicense = await GetFreeLicenseAsync(publishedFileDetails.consumer_appid, cancellationToken);

            if (!gotFreeLicense)
            {
                throw new SteamRequiredLicenseNotFoundException(publishedFileDetails.consumer_appid);
            }
        }

        manifestId ??= publishedFileDetails.hcontent_file;

        if (!manifestId.HasValue || manifestId.Value.Id == 0)
        {
            throw new SteamAccessDeniedException(
                "Cannot download workshop item because no manifest id was found (Maybe you don't have access to this workshop).");
        }

        return await GetAppDataInternalAsync(
            publishedFileDetails.consumer_appid,
            publishedFileDetails.consumer_appid,
            manifestId.Value,
            true,
            cancellationToken);
    }

    public async Task<IDownloadHandler> GetAppDataAsync(
        AppId appId,
        string branch = "public",
        string branchPassword = default,
        bool skipInaccessibleDepots = true,
        Func<Depot, bool> depotIdCondition = default,
        CancellationToken cancellationToken = default)
    {
        // Get all depots that are part of the branch
        var depots = await GetDepotsAsync(appId, branch, true, cancellationToken);

        // If the user specified a more detailed list of depots to download, filter for them
        depotIdCondition ??= x => true;
        depots = depots.Where(x => depotIdCondition.Invoke(x)).ToList();

        if (depots.Count == 0)
        {
            throw new SteamDownloadCriteriaHaveNoResultException(
                "No depot to download found that matches the criteria.");
        }

        var multipleFilesHandlers = new List<IDownloadHandler>();

        // For each depot, create an individual download handler that will download that depot
        foreach (var depot in depots)
        {
            try
            {
                if (!await GetHasAccessAsync(depot.Id, cancellationToken))
                {
                    throw new SteamDepotAccessDeniedException(depot.Id);
                }
                
                if (depot.Manifests.Any())
                {
                    var manifest = await GetManifestAsync(
                        appId,
                        depot.Id,
                        depot.Manifests.First().ManifestId,
                        cancellationToken);

                    multipleFilesHandlers.Add(new DefaultDownloadHandler(this, manifest, appId, depot.Id));
                }

                if (depot.EncryptedManifests.Any())
                {
                    if (branchPassword == null)
                    {
                        throw new SteamInvalidBranchPasswordException(appId, depot.Id, branch);
                    }


                    var decryptedManifestMeta = await DecryptDepotManifestAsync(
                        depot.EncryptedManifests.First(),
                        branchPassword);

                    var manifest = await GetManifestAsync(
                        appId,
                        depot.Id,
                        decryptedManifestMeta.ManifestId,
                        cancellationToken);

                    multipleFilesHandlers.Add(new DefaultDownloadHandler(this, manifest, appId, depot.Id));
                }
            }
            catch (SteamAccessDeniedException)
            {
                if (skipInaccessibleDepots)
                {
                    continue;
                }

                throw;
            }
        }

        return new MultiDepotDownloadHandler(multipleFilesHandlers);
    }

    public async Task<IDownloadHandler> GetAppDataAsync(
        AppId appId,
        DepotId depotId,
        string branch = "public",
        string branchPassword = default,
        bool skipInaccessibleDepots = true,
        CancellationToken cancellationToken = default)
    {
        return await GetAppDataAsync(
            appId,
            branch,
            branchPassword,
            skipInaccessibleDepots,
            x => x.Id == depotId,
            cancellationToken);
    }

    public async Task<IDownloadHandler> GetAppDataAsync(
        AppId appId,
        DepotId depotId,
        ManifestId manifestId,
        CancellationToken cancellationToken = default)
    {
        return await GetAppDataInternalAsync(
            appId,
            depotId,
            manifestId,
            false,
            cancellationToken);
    }

    private static SteamKit.KeyValue GetDepotsSection(
        SteamKit.SteamApps.PICSProductInfoCallback.PICSProductInfo appInfo,
        AppId appId)
    {
        return appInfo.KeyValues.Children.FirstOrDefault(x => x.Name == "depots")
               ?? throw new SteamDepotNotFoundException($"App {appId} has no depot information.");
    }

    public async Task<IReadOnlyList<Depot>> GetDepotsAsync(
        AppId appId,
        string branch = default,
        bool resolveSharedDepots = true,
        CancellationToken cancellationToken = default)
    {
        if (branch != null)
        {
            // Check if the branch exists.
            if ((await GetBranchesAsync(appId, cancellationToken)).All(
                    b => !string.Equals(b.Name, branch, StringComparison.OrdinalIgnoreCase)))
            {
                throw new SteamBranchNotFoundException(appId, branch);
            }
        }

        var appInfo = await GetAppInfoAsync(appId, cancellationToken);
        var allDepotData = GetDepotsSection(appInfo, appId);
        var filteredDepotData = new List<SteamKit.KeyValue>();
        var parsedDepots = new List<Depot>();

        foreach (var depotData in allDepotData.Children)
        {
            if (!uint.TryParse(depotData.Name, out var depotId))
            {
                // This is not a depot but instead meta information.
                // Maybe we'll be interested in it at a later point in development.
                continue;
            }

            var isSharedInstall = depotData["sharedinstall"].AsBoolean();
            AppId? depotFromApp = depotData["depotfromapp"].AsUnsignedInteger();

            if (depotFromApp.Value.Id == default)
            {
                depotFromApp = null;
            }

            // If this is a shared install, fetch the depot data separately, because this "link" to the actual depot
            // will not contain any manifests!
            if (isSharedInstall && resolveSharedDepots && depotFromApp.HasValue)
            {
                var sharedAppInfo = await GetAppInfoAsync(depotFromApp!.Value, cancellationToken);
                var sharedAppInfoDepots = GetDepotsSection(sharedAppInfo, depotFromApp.Value);

                foreach (var sharedAppInfoDepot in sharedAppInfoDepots.Children)
                {
                    if (uint.TryParse(sharedAppInfoDepot.Name, out var sharedAppInfoDepotId) &&
                        sharedAppInfoDepotId == depotId)
                    {
                        filteredDepotData.Add(sharedAppInfoDepot);
                    }
                }
            }
            else
            {
                filteredDepotData.Add(depotData);
            }
        }

        foreach (var depot in filteredDepotData)
        {
            var depotId = uint.Parse(depot.Name!);

            // Collect data
            var name = depot["name"].Value;
            var maxSize = depot["maxsize"].AsUnsignedLong();
            var isSharedInstall = depot["sharedinstall"].AsBoolean();
            var depotFromApp = (AppId?) depot["depotfromapp"].AsUnsignedInteger();

            var operatingSystems = new List<SteamOs>();
            var processorArchitectures = new List<string>();
            var languages = new List<string>();
            var configEntries = new Dictionary<string, string>();

            foreach (var configEntry in depot["config"].Children)
            {
                if (configEntry.Name == "oslist")
                {
                    operatingSystems.AddRange(
                        configEntry
                            .AsString()
                            .Split(',')
                            .Select(identifier => new SteamOs(identifier)));
                }
                else if (configEntry.Name == "osarch")
                {
                    processorArchitectures.AddRange(
                        configEntry
                            .AsString()
                            .Split(','));
                }
                else if (configEntry.Name == "language")
                {
                    languages.AddRange(
                        configEntry
                            .AsString()
                            .Split(','));
                }
                else
                {
                    configEntries[configEntry.Name] = configEntry.Value;
                }
            }

            var manifests = depot["manifests"]
                .Children
                .Select(x => new DepotManifest(x.Name, x["gid"].AsUnsignedLong()))
                .ToList();

            var encryptedManifests = depot["encryptedmanifests"]
                .Children
                .Select(
                    manifest =>
                    {
                        if (manifest["encrypted_gid_2"] != SteamKit.KeyValue.Invalid)
                        {
                            return new DepotEncryptedManifest(
                                appId,
                                depotId,
                                manifest.Name,
                                manifest["encrypted_gid_2"].Value,
                                DepotEncryptedManifest.EncryptionVersion.V2);
                        }

                        if (manifest["encrypted_gid"] != SteamKit.KeyValue.Invalid)
                        {
                            return new DepotEncryptedManifest(
                                appId,
                                depotId,
                                manifest.Name,
                                manifest["encrypted_gid"].Value,
                                DepotEncryptedManifest.EncryptionVersion.V1);
                        }

                        return new DepotEncryptedManifest(
                            appId,
                            depotId,
                            manifest.Name,
                            manifest["gid"].Value,
                            DepotEncryptedManifest.EncryptionVersion.V2);
                    })
                .ToList();


            // Optionally filter for a branch
            if (branch != default)
            {
                manifests = manifests
                    .Where(x => string.Equals(x.BranchName, branch, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                encryptedManifests = encryptedManifests
                    .Where(x => string.Equals(x.BranchName, branch, StringComparison.OrdinalIgnoreCase))
                    .ToList();
            }

            // If this depot has no manifests, skip it
            if (encryptedManifests.Count == 0 && manifests.Count == 0)
            {
                continue;
            }

            //// Filter out depots that do not match the given operating system
            //if (os != default && operatingSystems.All(x => x.Identifier != os.Identifier))
            //{
            //    continue;
            //}

            parsedDepots.Add(
                new Depot(
                    depotId,
                    name,
                    maxSize,
                    operatingSystems,
                    processorArchitectures,
                    languages,
                    manifests,
                    encryptedManifests,
                    configEntries,
                    isSharedInstall,
                    depotFromApp));
        }

        return parsedDepots;
    }

    public async Task<Depot> GetDepotAsync(
        AppId appId,
        DepotId depotId,
        CancellationToken cancellationToken = default)
    {
        var depot = (await GetDepotsAsync(appId, cancellationToken: cancellationToken))
            .FirstOrDefault(x => x.Id == depotId);

        if (depot == null)
        {
            throw new SteamDepotNotFoundException(depotId);
        }

        return depot;
    }

    public async Task ValidateBranchPasswordAsync(IEnumerable<Depot> depots, string branch, string branchPassword)
    {
        await ValidateBranchPasswordAsync(
            depots.SelectMany(
                x => x.EncryptedManifests.Where(
                    m => string.Equals(m.BranchName, branch, StringComparison.OrdinalIgnoreCase))),
            branchPassword);
    }

    public async Task ValidateBranchPasswordAsync(
        IEnumerable<DepotEncryptedManifest> manifests,
        string branchPassword)
    {
        foreach (var manifest in manifests)
        {
            if (branchPassword == null)
            {
                throw new ArgumentNullException(nameof(branchPassword));
            }

            await DecryptDepotManifestAsync(manifest, branchPassword);
        }
    }

    public async Task<IReadOnlyList<Branch>> GetBranchesAsync(
        AppId appId,
        CancellationToken cancellationToken = default)
    {
        var appInfo = await GetAppInfoAsync(appId, cancellationToken);
        var depots = GetDepotsSection(appInfo, appId);
        var branches = depots["branches"];

        var parsedBranches = new List<Branch>();

        foreach (var branch in branches.Children)
        {
            var name = branch.Name;
            var buildid = branch["buildid"].AsUnsignedLong();
            var description = branch["description"].AsString();
            var requiresPassword = branch["pwdrequired"].AsBoolean();
            var timeUpdated = branch["timeupdated"].AsLong();

            parsedBranches.Add(
                new Branch(
                    name,
                    buildid,
                    description,
                    requiresPassword,
                    DateTimeOffset.FromUnixTimeSeconds(timeUpdated)));
        }

        return parsedBranches;
    }

    [SuppressMessage("Security", "S5542", Justification = "Steam encrypts branch manifest ids with AES-ECB. The format is fixed by Steam.")]
    [SuppressMessage("Security", "CA5358", Justification = "Same reason: AES-ECB is dictated by Steam.")]

    public async Task<DepotManifest> DecryptDepotManifestAsync(
        DepotEncryptedManifest manifest,
        string branchPassword)
    {
        if (manifest.Version == DepotEncryptedManifest.EncryptionVersion.V1)
        {
            throw new NotSupportedException("Version 1 encrypted manifests are not supported.");
        }
        else
        {
            var result = await SteamApps.CheckAppBetaPassword(manifest.AppId, branchPassword);

            if (result.BetaPasswords.All(x => x.Key != manifest.BranchName))
            {
                throw new SteamInvalidBranchPasswordException(
                    manifest.AppId,
                    manifest.DepotId,
                    manifest.BranchName);
            }

            var encryptedManifestIdBytes = DecodeHexString(manifest.EncryptedManifestId);

            byte[] SymmetricDecryptECB(byte[] input, byte[] key)
            {
                using var aes = Aes.Create();
                aes.BlockSize = 128;
                aes.KeySize = 256;
                aes.Mode = CipherMode.ECB;
                aes.Padding = PaddingMode.PKCS7;

                using var aesTransform = aes.CreateDecryptor(key, null);
                var output = aesTransform.TransformFinalBlock(input, 0, input.Length);

                return output;
            }
            
            try
            {
                var manifestIdBytes = SymmetricDecryptECB(encryptedManifestIdBytes,
                    result.BetaPasswords.Single(x => x.Key == manifest.BranchName).Value);

                return new DepotManifest(manifest.BranchName, BitConverter.ToUInt64(manifestIdBytes));
            }
            catch (Exception ex)
            {
                throw new SteamInvalidBranchPasswordException(
                    manifest.AppId,
                    manifest.DepotId,
                    manifest.BranchName,
                    ex);
            }
        }
    }

    public async Task<ManifestId> GetDepotManifestIdAsync(
        AppId appId,
        DepotId depotId,
        string branch = "public",
        string branchPassword = default,
        CancellationToken cancellationToken = default)
    {
        // Enforce a default branch.
        // We can only return a manifest id if we know what branch to return for.
        branch ??= "public";

        var depots = await GetDepotsAsync(appId, branch, false, cancellationToken);
        var depot = depots.FirstOrDefault(x => x.Id == depotId);

        if (depot == null)
        {
            throw new SteamDownloadCriteriaHaveNoResultException(
                "No manifest could be found that matches all specified criteria.");
        }

        if (depot.IsSharedInstall)
        {
            // Rerun the method with the different app id
            return await GetDepotManifestIdAsync(
                depot.DepotOfAppId!.Value,
                depotId,
                branch,
                branchPassword,
                cancellationToken);
        }

        var manifest = depot.Manifests.FirstOrDefault();
        var encryptedManifest = depot.EncryptedManifests.FirstOrDefault();

        if (manifest != null)
        {
            return manifest.ManifestId;
        }

        if (encryptedManifest != null)
        {
            if (branchPassword == null)
            {
                throw new SteamInvalidBranchPasswordException(appId, depotId, branch);
            }

            manifest = await DecryptDepotManifestAsync(encryptedManifest, branchPassword);

            return manifest.ManifestId;
        }

        throw new ArgumentException(
            $"Combination of app id = {appId}, depot id = {depotId} and branch = {branch} could not be found.");
    }

    internal async Task<IDownloadHandler> GetAppDataInternalAsync(
        AppId appId,
        DepotId depotId,
        ManifestId manifestId,
        bool isUserGeneratedContent = false,
        CancellationToken cancellationToken = default)
    {
        if (!await GetHasAccessAsync(appId, cancellationToken))
        {
            var gotFreeLicense = await GetFreeLicenseAsync(appId, cancellationToken);

            if (!gotFreeLicense)
            {
                throw new SteamDepotAccessDeniedException(depotId);
            }
        }

        if (isUserGeneratedContent)
        {
            var appInfo = await GetAppInfoAsync(appId, cancellationToken);
            var depotsRaw = GetDepotsSection(appInfo, appId);

            depotId = depotsRaw["workshopdepot"].AsUnsignedInteger();

            return new DefaultDownloadHandler(
                this,
                await GetManifestAsync(
                    appId,
                    depotId,
                    manifestId,
                    cancellationToken),
                appId,
                depotId);
        }

        var depots = await GetDepotsAsync(appId, cancellationToken: cancellationToken);
        var depot = depots.FirstOrDefault(x => x.Id == depotId);

        if (depot == null)
        {
            throw new SteamDepotNotFoundException(depotId);
        }

        return new DefaultDownloadHandler(
            this,
            await GetManifestAsync(
                appId,
                depotId,
                manifestId,
                cancellationToken),
            appId,
            depotId);
    }

    internal async Task RequestAppAccessTokenIfNecessaryAsync(
        AppId appId,
        CancellationToken cancellationToken = default)
    {
        if (_appAccessTokens.ContainsKey(appId))
        {
            return;
        }

        var accessTokenRequestResult =
            await SteamApps.PICSGetAccessTokens(new List<uint> { appId }, new List<uint>())
                .ToTask()
                .WaitAsync(cancellationToken);

        if (accessTokenRequestResult.AppTokensDenied.Contains(appId))
        {
            throw new SteamAppAccessTokenDeniedException(appId);
        }

        _appAccessTokens.AddOrUpdate(
            appId,
            accessTokenRequestResult.AppTokens[appId],
            (appId, val) => accessTokenRequestResult.AppTokens[appId]);
    }

    internal async Task<IReadOnlyList<uint>> RequestPackageAccessTokenIfNecessaryAsync(
        IEnumerable<uint> packageIds,
        CancellationToken cancellationToken = default)
    {
        packageIds = packageIds.ToList();
        var grantedPackageIds = new List<uint>();

        if (packageIds.All(id => _packageAccessTokens.ContainsKey(id)))
        {
            return packageIds.ToList();
        }

        var accessTokenRequestResult =
            await SteamApps.PICSGetAccessTokens(new List<uint>(), packageIds)
                .ToTask()
                .WaitAsync(cancellationToken);

        foreach (var packageId in packageIds)
        {
            if (accessTokenRequestResult.PackageTokensDenied.Contains(packageId))
            {
                //throw new SteamAccessDeniedException($"Access denied to package id = {packageId}.");
                continue;
            }

            _packageAccessTokens.AddOrUpdate(
                packageId,
                accessTokenRequestResult.PackageTokens[packageId],
                (packageId, val) => accessTokenRequestResult.PackageTokens[packageId]);

            grantedPackageIds.Add(packageId);
        }

        return grantedPackageIds;
    }

    internal async Task<SteamKit.SteamApps.PICSProductInfoCallback.PICSProductInfo> GetAppInfoAsync(
        AppId appId,
        CancellationToken cancellationToken = default)
    {
        var result = await GetAppProductInfoAsync(new List<AppId> { appId }, false, cancellationToken);

        return result.First(x => x.ID == appId.Id);
    }

    internal async Task<List<SteamKit.SteamApps.PICSProductInfoCallback.PICSProductInfo>> GetAppProductInfoAsync(
        List<AppId> appIds,
        bool forceUpdate = true,
        CancellationToken cancellationToken = default)
    {
        var productInfos = new List<SteamKit.SteamApps.PICSProductInfoCallback.PICSProductInfo>();
        var appRequests = new List<SteamKit.SteamApps.PICSRequest>();

        foreach (var appId in appIds)
        {
            // Retrieve from cache if possible
            if (!forceUpdate && _appProductInfos.TryGetValue(appId, out var info))
            {
                productInfos.Add(info);
                continue;
            }

            // Fetch live

            await RequestAppAccessTokenIfNecessaryAsync(appId, cancellationToken);

            appRequests.Add(
                new SteamKit.SteamApps.PICSRequest
                {
                    ID = appId,
                    AccessToken = _appAccessTokens[appId]
                });
        }

        // Everything was in the cache, so there is nothing to ask Steam
        if (appRequests.Count == 0)
        {
            return productInfos;
        }

        var result = await SteamApps.PICSGetProductInfo(appRequests, new List<SteamKit.SteamApps.PICSRequest>())
            .ToTask()
            .WaitAsync(cancellationToken);

        // Steam can split a big answer into several parts, so read all of them
        foreach (var (key, value) in result.Results!.SelectMany(r => r.Apps))
        {
            _appProductInfos.AddOrUpdate(key, value, (ek, ev) => value);
            productInfos.Add(value);
        }

        return productInfos;
    }

        internal async Task<List<SteamKit.SteamApps.PICSProductInfoCallback.PICSProductInfo>>
        GetPackageProductInfoAsync(
            List<uint> packageIds,
            bool forceUpdate = false,
            CancellationToken cancellationToken = default)
    {
        var productInfos = new List<SteamKit.SteamApps.PICSProductInfoCallback.PICSProductInfo>();
        var missing = new List<uint>();

        foreach (var packageId in packageIds)
        {
            // Retrieve from cache if possible, only ask Steam about the rest
            if (!forceUpdate && _packageProductInfos.TryGetValue(packageId, out var info))
            {
                productInfos.Add(info);
            }
            else
            {
                missing.Add(packageId);
            }
        }

        if (missing.Count == 0)
        {
            return productInfos;
        }

        var grantedIds = await RequestPackageAccessTokenIfNecessaryAsync(missing, cancellationToken);

        var packageRequests = grantedIds
            .Select(id => new SteamKit.SteamApps.PICSRequest { ID = id, AccessToken = _packageAccessTokens[id] })
            .ToList();

        if (packageRequests.Count == 0)
        {
            return productInfos;
        }

        var result = await SteamApps.PICSGetProductInfo(new List<SteamKit.SteamApps.PICSRequest>(), packageRequests)
            .ToTask()
            .WaitAsync(cancellationToken);

        foreach (var (key, value) in result.Results!.SelectMany(r => r.Packages))
        {
            _packageProductInfos.AddOrUpdate(key, value, (ek, ev) => value);
            productInfos.Add(value);
        }

        return productInfos;
    }

    /// <summary>
    ///     Checks whether the signed in user has access to the given ID. Can be either an app ID or a depot ID.
    /// </summary>
    /// <param name="id">App ID or depot ID.</param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<bool> GetHasAccessAsync(
        uint id,
        CancellationToken cancellationToken = default)
    {
        // 17906 is the package id for downloading anonymous dedicated servers.
        // One package id grants access to many app ids.
        const uint dedicatedServerPackageId = 17906;

        var packageIds = SteamUser.SteamID!.AccountType == SteamKit.EAccountType.AnonUser
            ? new List<uint>()
            : SteamClient.Licenses.Select(x => x.PackageID).ToList();

        if (!packageIds.Contains(dedicatedServerPackageId))
        {
            packageIds.Add(dedicatedServerPackageId);
        }

        var packageInfos = await GetPackageProductInfoAsync(packageIds, false, cancellationToken);

        foreach (var packageId in packageIds)
        {
            var query = packageInfos.Where(x => x.ID == packageId).ToList();

            if (!query.Any())
            {
                continue;
            }

            var packageInfo = query.First();

            if (packageInfo.KeyValues["appids"].Children.Any(x => x.AsUnsignedInteger() == id))
            {
                return true;
            }

            if (packageInfo.KeyValues["depotids"].Children.Any(x => x.AsUnsignedInteger() == id))
            {
                return true;
            }
        }

        return false;
    }

    internal async Task<bool> GetFreeLicenseAsync(AppId appId, CancellationToken cancellationToken = default)
    {
        var result = await SteamApps.RequestFreeLicense(appId).ToTask().WaitAsync(cancellationToken);

        return result.GrantedApps.Contains(appId);
    }

    private static byte[] DecodeHexString(string hex)
    {
        if (hex == null)
        {
            return null;
        }

        var chars = hex.Length;
        var bytes = new byte[chars / 2];

        for (var i = 0; i < chars; i += 2)
        {
            bytes[i / 2] = Convert.ToByte(hex.Substring(i, 2), 16);
        }

        return bytes;
    }
}