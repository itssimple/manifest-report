using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.JsonDiffPatch;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Hangfire;
using Hangfire.Console;
using Hangfire.Server;
using Manifest.Report.Classes;

namespace Manifest.Report;

public partial class ManifestVersionArchiver(
    ILogger<ManifestVersionArchiver> logger,
    IHttpClientFactory httpClientFactory,
    AmazonS3Client s3Client,
    StoreManifestDiffs diffStore,
    string ghToken)
{
    public const string RootUrl = "https://www.bungie.net";
    public const string ApiBaseUrl = $"{RootUrl}/Platform";

    public const string ManifestUrl = $"{ApiBaseUrl}/Destiny2/Manifest/";

    private static readonly HashSet<string> ImageEndings = new([".png", ".jpg", ".jpeg", ".gif"]);

    private readonly List<string> IgnoredFiles = new()
        { "/done.txt", "/enhanced-manifest.json", "/manifest.json", ".content" };

    private PerformContext? _context;
    private HttpClient httpClient;

    private async Task<Destiny2Response<Destiny2Manifest>?> GetManifest()
    {
        logger.LogDebug("Trying to load manifest from {ManifestUrl}", ManifestUrl);
        _context?.WriteLine($"Trying to load manifest from {ManifestUrl}");

        var response = await httpClient.GetAsync($"{ManifestUrl}?_breakCache={DateTime.Now.Ticks}");

        if (!response.IsSuccessStatusCode)
        {
            var responseContent = await response.Content.ReadAsStringAsync();
            logger.LogError("Failed to load manifest from {ManifestUrl}", ManifestUrl);
            logger.LogError("Response: {Response}", responseContent);

            _context?.WriteLine($"Failed to load manifest from {ManifestUrl}", ManifestUrl);
            _context?.WriteLine($"Response: {responseContent}");

            return null;
        }

        var content = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<Destiny2Response<Destiny2Manifest>>(content);
    }

    public async Task<bool> CheckForNewManifest(PerformContext context)
    {
        _context = context;
        httpClient = httpClientFactory.CreateClient("Bungie");

        var manifest = await GetManifest();
        if (manifest == null || manifest.Response == null)
        {
            logger.LogError("Failed to load manifest");
            _context?.WriteLine("Failed to load manifest");
            return false;
        }

        var currentManifestVersion = GetVersionFromManifest(manifest.Response);

        if (currentManifestVersion == null)
        {
            logger.LogError("Failed to extract version from manifest");
            _context?.WriteLine("Failed to extract version from manifest");
            return false;
        }

        var previousManifestVersion = await GetPreviousManifestVersion();
        if (currentManifestVersion != previousManifestVersion)
        {
            await SaveManifest(currentManifestVersion, manifest.Response);

            logger.LogInformation("New manifest version found: {Version}", currentManifestVersion);
            _context?.WriteLine($"New manifest version found: {currentManifestVersion}");
            return true;
        }

        return false;
    }

    [GeneratedRegex(@"(\{){0,1}[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(\}){0,1}")]
    private static partial Regex GuidRegex();

    private Guid? GetVersionFromManifest(Destiny2Manifest manifest)
    {
        var match = GuidRegex().Match(manifest.JsonWorldContentPaths["en"]);

        if (!match.Success)
        {
            logger.LogError("Failed to extract version from manifest");
            _context?.WriteLine("Failed to extract version from manifest");
            return null;
        }

        return Guid.Parse(match.Value);
    }

    private async Task<Guid> GetPreviousManifestVersion()
    {
        // Load the previous manifest version from the database
        await Task.CompletedTask;
        return Guid.Empty;
    }

    private async Task<bool> FileExistsInStorage(string path)
    {
        try
        {
            await s3Client.GetObjectMetadataAsync("manifest-archive", path);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    private async Task SaveManifest(Guid? manifestVersionGuid, Destiny2Manifest manifest)
    {
        var downloadItems = new HashSet<(string url, string filePath)>();
        var discoverDate = DateTimeOffset.UtcNow;

        var versionFolder = $"versions/{manifestVersionGuid}";

        var sqliteFileName = manifest.MobileWorldContentPaths["en"].Split('/').Last();

        downloadItems.Add(($"{RootUrl}{manifest.MobileWorldContentPaths["en"]}", $"{versionFolder}/{sqliteFileName}"));
        foreach (var component in manifest.JsonWorldComponentContentPaths["en"])
            downloadItems.Add(($"{RootUrl}{component.Value}", $"{versionFolder}/tables/{component.Key}.json"));

        var existingVersion = await FileExistsInStorage($"{versionFolder}/manifest.json");

        if (!existingVersion)
        {
            logger.LogInformation(
                "New manifest-version found! Saving {VersionFolder} and downloading SQLite and json definitions",
                versionFolder);
            _context?.WriteLine(
                $"New manifest-version found! Saving {versionFolder} and downloading SQLite and json definitions");
        }

        if (await FileExistsInStorage($"{versionFolder}/done.txt"))
        {
            logger.LogInformation("Already downloaded all files, don't want to spam the poor server.");
            _context?.WriteLine("Already downloaded all files, don't want to spam the poor server.");
            return;
        }

        var restoredManifest = CleanManifest(manifestVersionGuid!.Value, manifest);
        var enhancedManifest = EnhanceManifest(manifestVersionGuid!.Value, restoredManifest);

        using var fw = new TransferUtility(s3Client);

        await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "manifest-archive",
            Key = $"{versionFolder}/manifest.json",
            ContentBody = JsonSerializer.Serialize(restoredManifest)
        });

        await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "manifest-archive",
            Key = $"{versionFolder}/enhanced-manifest.json",
            ContentBody = JsonSerializer.Serialize(enhancedManifest)
        });

        foreach (var (url, filePath) in downloadItems)
        {
            logger.LogDebug("- Downloading {Url} to {FilePath}", url, filePath);
            _context?.WriteLine($"- Downloading {url} to {filePath}");

            using var s = await httpClient.GetStreamAsync(url);
            await fw.UploadAsync(s, "manifest-archive", filePath);
        }

        logger.LogInformation("Manifest saved! Updating list.json with new info");
        _context?.WriteLine("Manifest saved! Updating list.json with new info");

        var listObj = await s3Client.GetObjectAsync("manifest-archive", "list.json");

        using var sr = new StreamReader(listObj.ResponseStream);
        var list = JsonSerializer.Deserialize<List<ManifestInfo>>(await sr.ReadToEndAsync())!;

        logger.LogInformation("Fetched previous version to find differences");
        _context?.WriteLine("Fetched previous version to find differences");

        var oldVersion = list.Last();

        var newVersion = new ManifestInfo
        {
            VersionId = manifestVersionGuid!.Value,
            Version = manifest.Version,
            ManifestJsonPath = $"/manifest-archive/{versionFolder}/manifest.json",
            EnhancedManifestJsonPath = $"/manifest-archive/{versionFolder}/enhanced-manifest.json",
            DiscoverDate_UTC = discoverDate
        };

        var versionParts = manifest.Version.Split('.').ToList();

        var year = $"20{versionParts[1]}";
        var month = versionParts[2];
        var day = versionParts[3];

        var time = versionParts[4].Substring(0, 4).Insert(2, ":");

        var dateFormat = $"{year}-{month}-{day} {time}:00Z";

        _ = DateTimeOffset.TryParse(dateFormat, out var manifestDate);

        newVersion.ManifestDate_UTC = manifestDate;

        var oldMan = oldVersion.VersionId;
        var newMan = newVersion.VersionId;

        var oldFiles = await FetchAllVersionFiles(oldMan);
        var newFiles = await FetchAllVersionFiles(newMan);

        var addedFiles = FindNewFiles(oldFiles, newFiles, $"versions/{oldMan}", $"versions/{newMan}");
        var removedFiles = FindRemovedFiles(oldFiles, newFiles, $"versions/{oldMan}", $"versions/{newMan}");
        var modifiedFiles = FindModifiedFiles(oldFiles, newFiles, $"versions/{oldMan}", $"versions/{newMan}");

        var changes =
            await FindDiffsBetweenFiles(newVersion, modifiedFiles, oldFiles, newFiles, addedFiles, removedFiles);

        newVersion.DiffFiles = changes;

        list.Add(newVersion);

        await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "manifest-archive",
            Key = "list.json",
            ContentBody = JsonSerializer.Serialize(list)
        });

        logger.LogInformation("List updated!");
        _context?.WriteLine("List updated!");

        await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "manifest-archive",
            Key = $"{versionFolder}/done.txt",
            ContentBody = "Done"
        });

        logger.LogInformation("Triggering new site.manifest.report generation");
        _context?.WriteLine("Triggering new site.manifest.report generation");

        var ghUrl = new Uri("https://api.github.com/repos/itssimple/manifest-report-site/dispatches");
        var request = new HttpRequestMessage(HttpMethod.Post, ghUrl);
        request.Headers.Add("Accept", "application/vnd.github+json");
        request.Headers.Add("X-GitHub-Api-Version", "2022-11-28");
        request.Headers.Add("Authorization", "Bearer " + ghToken);
        request.Headers.Add("User-Agent", "Manifest.report-triggerer");
        request.Content =
            new StringContent(
                JsonSerializer.Serialize(new
                {
                    event_type = "deploy",
                    client_payload = new { message = "New manifest version, generating new static site" }
                }), Encoding.UTF8, "application/json");

        var res = await httpClient.SendAsync(request);
        if (res.IsSuccessStatusCode)
        {
            logger.LogInformation("site.manifest.report generation triggered!");
            _context?.WriteLine("site.manifest.report generation triggered!");
        }
        else
        {
            logger.LogError("Failed to trigger site.manifest.report generation!");
            _context?.WriteLine("Failed to trigger site.manifest.report generation!");
        }

        logger.LogInformation("Finding and storing all images from the definitions");
        _context?.WriteLine("Finding and storing all images from the definitions");

        var CONCURRENCY_LEVEL = Environment.ProcessorCount / 4;
        if (CONCURRENCY_LEVEL < 4) CONCURRENCY_LEVEL = 4;

        var fetchTasks = new List<Task>();

        var allImages = new BlockingCollection<string>();

        await FetchImagesFromManifest(newVersion, allImages);

        var nextIndex = 0;

        while (fetchTasks.Count > 0)
        {
            try
            {
                var dt = await Task.WhenAny(fetchTasks);
                fetchTasks.Remove(dt);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred while fetching images from manifest");
                _context?.WriteLine("An error occurred while fetching images from manifest");
            }

            if (nextIndex < list.Count)
            {
                fetchTasks.Add(FetchImagesFromManifest(list[nextIndex], allImages));
                nextIndex++;
            }
        }

        var listImages = allImages.Distinct().ToList();

        nextIndex = 0;

        while (nextIndex < CONCURRENCY_LEVEL && nextIndex < listImages.Count)
        {
            fetchTasks.Add(FetchImageFromBungie(httpClient, listImages[nextIndex]));
            nextIndex++;
        }

        while (fetchTasks.Count > 0)
        {
            try
            {
                var dt = await Task.WhenAny(fetchTasks);
                fetchTasks.Remove(dt);
            }
            catch (Exception ex)
            {
            }

            if (nextIndex < listImages.Count)
            {
                fetchTasks.Add(FetchImageFromBungie(httpClient, listImages[nextIndex]));
                nextIndex++;
            }
        }

        BackgroundJob.Enqueue(() => diffStore.StoreDiffs(null, JobCancellationToken.Null));
    }

    private async Task<bool> CheckIfFileExists(string bucketName, string key)
    {
        try
        {
            await s3Client.GetObjectMetadataAsync(bucketName, key);

            return true;
        }
        catch (AmazonS3Exception s3)
        {
            if (s3.StatusCode == HttpStatusCode.NotFound) return false;
        }

        return false;
    }

    private async Task FetchImagesFromManifest(ManifestInfo manifest, BlockingCollection<string> allImages)
    {
        var newMan = manifest.VersionId;
        var newFiles = await FetchAllVersionFiles(newMan);

        foreach (var file in newFiles)
        {
            var definitionJson = await GetFileAsJsonNode(file);
            var foundImages = await FindImagesInJson(definitionJson);

            foreach (var image in foundImages)
            {
                if (!allImages.Contains(image))
                    allImages.Add(image);
            }
        }
    }

    private static bool IsImageValue(string value)
    {
        return ImageEndings.Any(value.EndsWith);
    }

    private async Task<JsonNode?> GetFileAsJsonNode(S3Object file)
    {
        var fileFetch = await s3Client.GetObjectAsync(file.BucketName, file.Key);
        using var sr = new StreamReader(fileFetch.ResponseStream);
        var json = await sr.ReadToEndAsync();
        var node = JsonSerializer.Deserialize<JsonNode>(json);
        return node;
    }

    private static async Task<HashSet<string>> FindImagesInJson(JsonNode definitionJson)
    {
        var images = new HashSet<string>();
        await Task.CompletedTask;

        var allProperties = definitionJson.Descendants(x =>
            x is JsonValue value && value.GetValueKind() == JsonValueKind.String &&
            IsImageValue(value.GetValue<string>()));

        foreach (var property in allProperties) images.Add(property.GetValue<string>());

        return images;
    }

    private async Task FetchImageFromBungie(HttpClient hc, string image)
    {
        var trimmedImage = image.TrimStart('/');

        var s3Key = $"images/{trimmedImage}";
        if (!await CheckIfFileExists("manifest-archive", s3Key))
        {
            var response = await hc.GetAsync($"https://www.bungie.net/{trimmedImage}");
            if (response.IsSuccessStatusCode)
            {
                await s3Client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = "manifest-archive",
                    Key = s3Key,
                    InputStream = response.Content.ReadAsStream()
                });
            }
        }
    }

    private async Task<List<S3Object>> FetchAllVersionFiles(Guid version)
    {
        ListObjectsV2Response fileResp;
        var request = new ListObjectsV2Request
        {
            BucketName = "manifest-archive",
            Prefix = $"/versions/{version}/tables"
        };

        var files = new List<S3Object>();

        do
        {
            fileResp = await s3Client.ListObjectsV2Async(request);
            files.AddRange(fileResp.S3Objects.Where(f => !f.Key.Contains("/diffFiles/")));

            request.ContinuationToken = fileResp.NextContinuationToken;
        } while (fileResp.IsTruncated);

        return files;
    }

    private Destiny2Manifest CleanManifest(Guid id, Destiny2Manifest manifest)
    {
        var clean = manifest.Clone();

        var fileName = clean.MobileWorldContentPaths["en"].Split('/').Last();
        var otherPath = clean.MobileWorldContentPaths["fr"].Split('/')[..^1];
        clean.MobileWorldContentPaths["en"] = string.Join("/", otherPath).Replace("/fr", $"/en/{fileName}");

        for (var i = 0; i < clean.JsonWorldComponentContentPaths["en"].Keys.Count; i++)
        {
            var key = clean.JsonWorldComponentContentPaths["en"].Keys.ElementAt(i);
            clean.JsonWorldComponentContentPaths["en"][key] = $"/common/destiny2_content/json/en/{key}-{id}.json";
        }

        return clean;
    }

    private Destiny2ManifestEnhanced EnhanceManifest(Guid id, Destiny2Manifest manifest)
    {
        var enhanced = new Destiny2ManifestEnhanced
        {
            Version = manifest.Version,
            MobileWorldContentPaths = manifest.MobileWorldContentPaths.Where(k => k.Key == "en")
                .ToDictionary(k => k.Key, v => v.Value),
            JsonWorldComponentContentPaths = manifest.JsonWorldComponentContentPaths.Where(k => k.Key == "en")
                .ToDictionary(k => k.Key, v => v.Value)
        };

        var fileName = enhanced.MobileWorldContentPaths["en"].Split('/').Last();
        enhanced.MobileWorldContentPaths["en"] = $"/manifest-archive/versions/{id}/{fileName}";

        var tables = enhanced.JsonWorldComponentContentPaths["en"];

        enhanced.JsonWorldComponentContentPaths["en"] = tables.ToDictionary(k => k.Key,
            v => $"/manifest-archive/versions/{id}/tables/{v.Key}.json");

        return enhanced;
    }

    public async Task<List<FileDiff>> FindDiffsBetweenFiles(ManifestInfo newVersion, List<string> diffFiles,
        List<S3Object> previousFiles, List<S3Object> currentFiles, List<string> addedFiles, List<string> removedFiles)
    {
        var changes = new List<FileDiff>();

        async Task WriteDiffFile(string diffFile, FileStatus fileStatus, JsonNode? prevJson, JsonNode? currJson)
        {
            var formatter = new DestinyJsonPatchDeltaFormatter();

            var diff = prevJson.Diff(currJson, formatter)!;
            if (formatter.Changes.Changes > 0)
            {
                changes.Add(new FileDiff
                {
                    FileName = diffFile,
                    EnhancedFileName =
                        $"/manifest-archive/versions/{newVersion.VersionId.ToString()}/diffFiles/{diffFile.Split('/').Last()}",
                    Added = formatter.Changes.Added.Count,
                    Modified = formatter.Changes.Modified.Count - formatter.Changes.Unclassified.Count -
                               formatter.Changes.Reclassified.Count - formatter.Changes.Added.Count -
                               formatter.Changes.Removed.Count,
                    Unclassified = formatter.Changes.Unclassified.Count,
                    Reclassified = formatter.Changes.Reclassified.Count,
                    Removed = formatter.Changes.Removed.Count,
                    FileStatus = fileStatus
                });

                await s3Client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = "manifest-archive",
                    Key = $"versions/{newVersion.VersionId.ToString()}/diffFiles/{diffFile.Split('/').Last()}",
                    ContentBody = diff.ToJsonString(new JsonSerializerOptions
                    {
                        WriteIndented = true
                    })
                });
            }
        }

        foreach (var diffFile in diffFiles)
        {
            var prevJson = await GetFileAsJsonNode(diffFile, previousFiles);
            var currJson = await GetFileAsJsonNode(diffFile, currentFiles);

            await WriteDiffFile(diffFile, FileStatus.Modified, prevJson, currJson);
        }

        foreach (var added in addedFiles)
        {
            var prevJson = new JsonObject();
            var currJson = await GetFileAsJsonNode(added, currentFiles);

            await WriteDiffFile(added, FileStatus.Added, prevJson, currJson);
        }

        foreach (var removed in removedFiles)
        {
            var prevJson = await GetFileAsJsonNode(removed, previousFiles);
            var currJson = new JsonObject();

            await WriteDiffFile(removed, FileStatus.Removed, prevJson, currJson);
        }

        return changes;
    }

    private async Task<JsonNode> GetFileAsJsonNode(string diffFile, List<S3Object> files)
    {
        var file = files.FirstOrDefault(f => f.Key.EndsWith(diffFile));
        var fileFetch = await s3Client.GetObjectAsync(file.BucketName, file.Key);
        using var sr = new StreamReader(fileFetch.ResponseStream);
        var json = await sr.ReadToEndAsync();
        var node = JsonSerializer.Deserialize<JsonNode>(json);
        return node;
    }

    public List<string> FindNewFiles(List<S3Object> previousFiles, List<S3Object> currentFiles, string prevPrefix,
        string currPrefix)
    {
        var previousExceptIgnored = previousFiles.Where(o => !IgnoredFiles.Any(s => o.Key.EndsWith(s)))
            .Select(s => s.Key.Replace(prevPrefix, "")).ToList();
        var currentExceptIgnored = currentFiles.Where(o => !IgnoredFiles.Any(s => o.Key.EndsWith(s)))
            .Select(s => s.Key.Replace(currPrefix, "")).ToList();

        var filesNotFoundInPrevious = currentExceptIgnored.Except(previousExceptIgnored).ToList();

        return filesNotFoundInPrevious;
    }

    public List<string> FindRemovedFiles(List<S3Object> previousFiles, List<S3Object> currentFiles, string prevPrefix,
        string currPrefix)
    {
        var previousExceptIgnored = previousFiles.Where(o => !IgnoredFiles.Any(s => o.Key.EndsWith(s)))
            .Select(s => s.Key.Replace(prevPrefix, "")).ToList();
        var currentExceptIgnored = currentFiles.Where(o => !IgnoredFiles.Any(s => o.Key.EndsWith(s)))
            .Select(s => s.Key.Replace(currPrefix, "")).ToList();

        var filesRemovedInCurrent = previousExceptIgnored.Except(currentExceptIgnored).ToList();

        return filesRemovedInCurrent;
    }

    public List<string> FindModifiedFiles(List<S3Object> previousFiles, List<S3Object> currentFiles, string prevPrefix,
        string currPrefix)
    {
        var previousExceptIgnored = previousFiles
            .Where(o => !IgnoredFiles.Any(s => o.Key.EndsWith(s))).Select(s => new
            {
                File = s.Key.Replace(prevPrefix, ""),
                s.ETag,
                s.Size
            }).ToList();

        var currentExceptIgnored = currentFiles
            .Where(o => !IgnoredFiles.Any(s => o.Key.EndsWith(s))).Select(s => new
            {
                File = s.Key.Replace(currPrefix, ""),
                s.ETag,
                s.Size
            }).ToList();

        var joined = from curr in currentExceptIgnored
                     join prev in previousExceptIgnored on curr.File equals prev.File
                     select new
                     {
                         Curr = curr,
                         Prev = prev
                     };

        return joined /*.Where(f => f.Curr.ETag != f.Prev.ETag || f.Curr.Size != f.Prev.Size)*/.Select(s => s.Curr.File)
            .ToList();
    }
}