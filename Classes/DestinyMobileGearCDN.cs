using System.Text.Json.Serialization;

namespace Manifest.Report.Classes
{
    public class Destiny2MobileGearCDN
    {
        [JsonPropertyName("Geometry")]
        public required string Geometry { get; set; }
        [JsonPropertyName("Texture")]
        public required string Texture { get; set; }
        [JsonPropertyName("PlateRegion")]
        public required string PlateRegion { get; set; }
        [JsonPropertyName("Gear")]
        public required string Gear { get; set; }
        [JsonPropertyName("Shader")]
        public required string Shader { get; set; }
    }
}
