using System.Text.Json.Serialization;

namespace Manifest.Report.Classes
{
    public class Destiny2MobileGearAssetDataBase
    {
        [JsonPropertyName("version")]
        public int Version { get; set; }
        [JsonPropertyName("path")]
        public required string Path { get; set; }
    }
}
