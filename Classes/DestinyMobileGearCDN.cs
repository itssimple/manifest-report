using System.Text.Json.Serialization;

namespace Manifest.Report.Classes
{
    public class Destiny2MobileGearCDN
    {
        [JsonPropertyName("Geometry")]
        public string Geometry { get; set; }
        [JsonPropertyName("Texture")]
        public string Texture { get; set; }
        [JsonPropertyName("PlateRegion")]
        public string PlateRegion { get; set; }
        [JsonPropertyName("Gear")]
        public string Gear { get; set; }
        [JsonPropertyName("Shader")]
        public string Shader { get; set; }
    }
}
