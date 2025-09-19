using System.Data;
using System.Text.Json.Serialization;

namespace Manifest.Report.Classes.DBClasses
{
    public class DestinyDefinitionHashHistoryCollectionItem
    {
        public DestinyDefinitionHashHistoryCollectionItem() { }

        public DestinyDefinitionHashHistoryCollectionItem(DataRow row)
        {
            HistoryId = row.Field<long>("HistoryId");
            Definition = row.Field<string>("Definition");
            Hash = row.Field<long>("Hash");
            ManifestVersion = row.Field<Guid>("ManifestVersion");
            DiscoveredUTC = row.Field<DateTimeOffset>("DiscoveredUTC");
            JSONContent = row.Field<string>("JSONContent");
            JSONDiff = row.Field<string>("JSONDiff");
            State = Enum.Parse<DefinitionHashChangeState>(row.Field<string>("State"));
        }

        [JsonPropertyName("HistoryId")]
        public long HistoryId { get; private set; }
        [JsonPropertyName("Definition")]
        public string Definition { get; set; }
        [JsonPropertyName("Hash")]
        public long Hash { get; set; }
        [JsonPropertyName("ManifestVersion")]
        public Guid ManifestVersion { get; set; }
        [JsonPropertyName("DiscoveredUTC")]
        public DateTimeOffset DiscoveredUTC { get; set; }
        [JsonPropertyName("JSONContent")]
        public string JSONContent { get; set; }
        [JsonPropertyName("JSONDiff")]
        public string JSONDiff { get; set; }
        [JsonPropertyName("State")]
        public DefinitionHashChangeState State { get; set; }
        [JsonIgnore]
        public bool IsDirty { get; set; } = false;
    }
}
