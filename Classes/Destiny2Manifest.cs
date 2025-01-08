using System.Text.Json.Serialization;
using System.Text.Json;

namespace Manifest.Report.Classes
{
    public class Destiny2Manifest
    {
        [JsonPropertyName("version")]
        public string Version { get; set; }
        [JsonPropertyName("mobileAssetContentPath")]
        public string MobileAssetContentPath { get; set; }
        [JsonPropertyName("mobileGearAssetDataBases")]
        public List<Destiny2MobileGearAssetDataBase> MobileGearAssetDataBases { get; set; }
        [JsonPropertyName("mobileWorldContentPaths")]
        public Dictionary<string, string> MobileWorldContentPaths { get; set; }
        [JsonPropertyName("jsonWorldContentPaths")]
        public Dictionary<string, string> JsonWorldContentPaths { get; set; }
        [JsonPropertyName("jsonWorldComponentContentPaths")]
        public Dictionary<string, Dictionary<string, string>> JsonWorldComponentContentPaths { get; set; }
        [JsonPropertyName("mobileClanBannerDatabasePath")]
        public string MobileClanBannerDatabasePath { get; set; }
        [JsonPropertyName("mobileGearCDN")]
        public Destiny2MobileGearCDN MobileGearCDN { get; set; }

        [JsonExtensionData]
        public Dictionary<string, JsonElement> ExtraData { get; set; }
    }
}
