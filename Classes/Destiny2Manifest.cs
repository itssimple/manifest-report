using System.Text.Json.Serialization;
using System.Text.Json;

namespace Manifest.Report.Classes
{
    public class Destiny2Manifest
    {
        [JsonPropertyName("version")]
        public required string Version { get; set; }
        [JsonPropertyName("mobileAssetContentPath")]
        public required string MobileAssetContentPath { get; set; }
        [JsonPropertyName("mobileGearAssetDataBases")]
        public required List<Destiny2MobileGearAssetDataBase> MobileGearAssetDataBases { get; set; }
        [JsonPropertyName("mobileWorldContentPaths")]
        public required Dictionary<string, string> MobileWorldContentPaths { get; set; }
        [JsonPropertyName("jsonWorldContentPaths")]
        public required Dictionary<string, string> JsonWorldContentPaths { get; set; }
        [JsonPropertyName("jsonWorldComponentContentPaths")]
        public required Dictionary<string, Dictionary<string, string>> JsonWorldComponentContentPaths { get; set; }
        [JsonPropertyName("mobileClanBannerDatabasePath")]
        public required string MobileClanBannerDatabasePath { get; set; }
        [JsonPropertyName("mobileGearCDN")]
        public required Destiny2MobileGearCDN MobileGearCDN { get; set; }

        [JsonExtensionData]
        public Dictionary<string, JsonElement>? ExtraData { get; set; }
    }
}
