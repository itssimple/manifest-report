using System.Data;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Manifest.Report.Classes.DBClasses
{
    public class D1DefinitionData
    {
        [JsonPropertyName("definition")]
        public string Definition { get; set; }
        [JsonPropertyName("hash")]
        public long Hash { get; set; }
        [JsonPropertyName("displayName"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string DisplayName {  get; set; }
        [JsonPropertyName("description"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string Description { get; set; }
        [JsonPropertyName("displayIcon"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string DisplayIcon { get; set; }

        [JsonIgnore]
        public string JSONContent { get; set; }

        [JsonPropertyName("data")]
        public JsonNode Data { get; set; }

        public D1DefinitionData(DataRow row)
        {
            Definition = row.Field<string>("Definition");
            Hash = row.Field<long>("Hash");
            JSONContent = row.Field<string>("JSONContent");
            if (!string.IsNullOrWhiteSpace(JSONContent))
            {
                Data = JsonSerializer.Deserialize<JsonNode>(JSONContent);
            }

            DisplayName = row.Field<string>("DisplayName");
            Description = row.Field<string>("Description");
            DisplayIcon = row.Field<string>("DisplayIcon");
        }
    }
}
