using Manifest.Report.Classes;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Manifest.Report
{
    public partial class ManifestVersionArchiver
    {
        public const string RootUrl = "https://www.bungie.net";
        public const string ApiBaseUrl = $"{RootUrl}/Platform";

        public const string ManifestUrl = $"{ApiBaseUrl}/Destiny2/Manifest/";
        private readonly ILogger<ManifestVersionArchiver> _logger;
        private readonly HttpClient _httpClient;

        public ManifestVersionArchiver(ILogger<ManifestVersionArchiver> logger, HttpClient httpClient)
        {
            _logger = logger;
            _httpClient = httpClient;
        }

        private async Task<Destiny2Response<Destiny2Manifest>?> GetManifest()
        {
            _logger.LogDebug("Trying to load manifest from {ManifestUrl}", ManifestUrl);
            var response = await _httpClient.GetAsync(ManifestUrl);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError("Failed to load manifest from {ManifestUrl}", ManifestUrl);
                _logger.LogError("Response: {Response}", await response.Content.ReadAsStringAsync());

                return null;
            }

            var content = await response.Content.ReadAsStringAsync();
            return JsonSerializer.Deserialize<Destiny2Response<Destiny2Manifest>>(content);
        }

        public async Task<bool> CheckForNewManifest()
        {
            var manifest = await GetManifest();
            if (manifest == null)
            {
                _logger.LogError("Failed to load manifest");
                return false;
            }

            var currentManifestVersion = GetVersionFromManifest(manifest.Response);

            if (currentManifestVersion == null)
            {
                _logger.LogError("Failed to extract version from manifest");
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
                _logger.LogError("Failed to extract version from manifest");
                return null;
            }

            return Guid.Parse(match.Value);
        }

        private async Task<Guid> GetPreviousManifestVersion()
        {
            // Load the previous manifest version from the database
            return Guid.Empty;
        }

        private async Task SaveManifest(Guid? manifestVersionGuid, Destiny2Manifest manifest)
        {
            // Save the manifest
            var downloadItems = new HashSet<(string url, string filePath)>();

            var versionFolder = $"versions/{manifestVersionGuid}";

            downloadItems.Add((ManifestUrl, $"{versionFolder}/manifest.json"));

            var sqliteFileName = manifest.MobileWorldContentPaths["en"].Split('/').Last();

            downloadItems.Add(($"{RootUrl}{manifest.MobileWorldContentPaths["en"]}", $"{versionFolder}/{sqliteFileName}"));
            foreach (var component in manifest.JsonWorldComponentContentPaths["en"])
            {
                downloadItems.Add(($"{RootUrl}{component.Value}", $"{versionFolder}/tables/{component.Key}.json"));
            }

            if (!Directory.Exists(Path.Combine(rootDir, versionFolder)))
            {
                _logger.LogInformation($"New manifest-version found! Saving {versionFolder} and downloading SQLite and json definitions");
            }

            if (File.Exists(Path.Combine(rootDir, versionFolder, "done.txt")))
            {
                _logger.LogInformation("Already downloaded all files, don't want to spam the poor server.");
                return;
            }

            foreach (var item in downloadItems)
            {
                var dlPath = Path.Combine(rootDir, item.filePath);

                Directory.CreateDirectory(Path.Combine(rootDir, versionFolder));
                Directory.CreateDirectory(Path.Combine(rootDir, versionFolder, "tables"));

                _logger.LogDebug($"- Downloading {item.url} to {item.filePath}");

                using var fw = new FileStream(dlPath, FileMode.Create);
                using var s = await _httpClient.GetStreamAsync(item.url);
                s.CopyTo(fw);

                _logger.LogDebug($"- Downloaded {new FileInfo(dlPath).Length} bytes from {item.url}");
            }

            File.WriteAllText(Path.Combine(rootDir, versionFolder, "done.txt"), "Done");
        }
    }
}
