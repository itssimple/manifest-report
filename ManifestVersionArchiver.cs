using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Manifest.Report.Classes;
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace Manifest.Report
{
    public partial class ManifestVersionArchiver(ILogger<ManifestVersionArchiver> logger, HttpClient httpClient, AmazonS3Client s3Client)
    {
        public const string RootUrl = "https://www.bungie.net";
        public const string ApiBaseUrl = $"{RootUrl}/Platform";

        public const string ManifestUrl = $"{ApiBaseUrl}/Destiny2/Manifest/";

        private async Task<Destiny2Response<Destiny2Manifest>?> GetManifest()
        {
            logger.LogDebug("Trying to load manifest from {ManifestUrl}", ManifestUrl);
            var response = await httpClient.GetAsync(ManifestUrl);

            if (!response.IsSuccessStatusCode)
            {
                logger.LogError("Failed to load manifest from {ManifestUrl}", ManifestUrl);
                logger.LogError("Response: {Response}", await response.Content.ReadAsStringAsync());

                return null;
            }

            var content = await response.Content.ReadAsStringAsync();
            return JsonSerializer.Deserialize<Destiny2Response<Destiny2Manifest>>(content);
        }

        public async Task<bool> CheckForNewManifest()
        {
            var manifest = await GetManifest();
            if (manifest == null || manifest.Response == null)
            {
                logger.LogError("Failed to load manifest");
                return false;
            }

            var currentManifestVersion = GetVersionFromManifest(manifest.Response);

            if (currentManifestVersion == null)
            {
                logger.LogError("Failed to extract version from manifest");
                return false;
            }

            var previousManifestVersion = await GetPreviousManifestVersion();
            if (currentManifestVersion != previousManifestVersion)
            {
                await SaveManifest(currentManifestVersion, manifest.Response);
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

            var versionFolder = $"versions/{manifestVersionGuid}";

            var sqliteFileName = manifest.MobileWorldContentPaths["en"].Split('/').Last();

            downloadItems.Add(($"{RootUrl}{manifest.MobileWorldContentPaths["en"]}", $"{versionFolder}/{sqliteFileName}"));
            foreach (var component in manifest.JsonWorldComponentContentPaths["en"])
            {
                downloadItems.Add(($"{RootUrl}{component.Value}", $"{versionFolder}/tables/{component.Key}.json"));
            }

            var existingVersion = await FileExistsInStorage($"{versionFolder}/manifest.json");

            if (!existingVersion)
            {
                logger.LogInformation("New manifest-version found! Saving {VersionFolder} and downloading SQLite and json definitions", versionFolder);
            }

            if (await FileExistsInStorage($"{versionFolder}/done.txt"))
            {
                logger.LogInformation("Already downloaded all files, don't want to spam the poor server.");
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

                using var s = await httpClient.GetStreamAsync(url);
                await fw.UploadAsync(s, "manifest-archive", filePath);
            }

            await s3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = "manifest-archive",
                Key = $"{versionFolder}/done.txt",
                ContentBody = "Done"
            });
        }

        private Destiny2Manifest CleanManifest(Guid id, Destiny2Manifest manifest)
        {
            var clean = manifest.Clone();

            var fileName = clean.MobileWorldContentPaths["en"].Split('/').Last();
            var otherPath = clean.MobileWorldContentPaths["fr"].Split('/')[0..^1];
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
            Destiny2ManifestEnhanced enhanced = new Destiny2ManifestEnhanced
            {
                Version = manifest.Version,
                MobileWorldContentPaths = manifest.MobileWorldContentPaths.Where(k => k.Key == "en").ToDictionary(k => k.Key, v => v.Value),
                JsonWorldComponentContentPaths = manifest.JsonWorldComponentContentPaths.Where(k => k.Key == "en").ToDictionary(k => k.Key, v => v.Value)
            };

            var fileName = enhanced.MobileWorldContentPaths["en"].Split('/').Last();
            enhanced.MobileWorldContentPaths["en"] = $"/manifest-archive/versions/{id}/{fileName}";

            var tables = enhanced.JsonWorldComponentContentPaths["en"];

            enhanced.JsonWorldComponentContentPaths["en"] = tables.ToDictionary(k => k.Key, v => $"/manifest-archive/versions/{id}/tables/{v.Key}.json");

            return enhanced;
        }
    }

    public class Destiny2ManifestEnhanced
    {
        [JsonPropertyName("version")]
        public string Version { get; set; }
        [JsonPropertyName("mobileWorldContentPaths")]
        public Dictionary<string, string> MobileWorldContentPaths { get; set; }
        [JsonPropertyName("jsonWorldComponentContentPaths")]
        public Dictionary<string, Dictionary<string, string>> JsonWorldComponentContentPaths { get; set; }
    }
}
