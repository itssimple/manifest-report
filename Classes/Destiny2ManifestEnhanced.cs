using System.Text.Json.Serialization;

namespace Manifest.Report.Classes
{
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
