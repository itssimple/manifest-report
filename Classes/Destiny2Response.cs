using System.Text.Json.Serialization;
using System.Text.Json;

namespace Manifest.Report.Classes
{
    public class Destiny2Response<T>
    {
        [JsonPropertyName("Response")]
        public T? Response { get; set; }
        [JsonPropertyName("ErrorCode")]
        public int ErrorCode { get; set; }
        [JsonPropertyName("ThrottleSeconds")]
        public int ThrottleSeconds { get; set; }
        [JsonPropertyName("ErrorStatus")]
        public required string ErrorStatus { get; set; }
        [JsonPropertyName("Message")]
        public required string Message { get; set; }
        [JsonPropertyName("MessageData")]
        public JsonElement MessageData { get; set; }
    }
}
