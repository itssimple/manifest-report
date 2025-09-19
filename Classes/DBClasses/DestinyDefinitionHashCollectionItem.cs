using System.Data;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Manifest.Report.Classes.DBClasses
{
    public class DestinyDefinitionHashCollectionItem
    {
        public DestinyDefinitionHashCollectionItem() { }

        public DestinyDefinitionHashCollectionItem(DataRow row)
        {
            HashCollectionId = row.Field<long>("HashCollectionId");
            Definition = row.Field<string>("Definition");
            Hash = row.Field<long>("Hash");
            FirstDiscoveredUTC = row.Field<DateTimeOffset>("FirstDiscoveredUTC");
            LatestManifestDateUTC = row.Field<DateTimeOffset>("LatestManifestDateUTC");
            RemovedUTC = row.Field<DateTimeOffset?>("RemovedUTC");
            DisplayName = row.Field<string>("DisplayName");
            DisplayIcon = row.Field<string>("DisplayIcon");
            InVersions = JsonSerializer.Deserialize<HashSet<Guid>>(row.Field<string>("InVersions")) ?? [];
            JSONContent = row.Field<string>("JSONContent");
        }

        [JsonPropertyName("HashCollectionId")]
        public long HashCollectionId { get; private set; }
        [JsonPropertyName("Definition")]
        public string Definition { get; set; }
        [JsonPropertyName("Hash")]
        public long Hash { get; set; }
        [JsonPropertyName("FirstDiscoveredUTC")]
        public DateTimeOffset? FirstDiscoveredUTC { get; set; }
        [JsonPropertyName("LatestManifestDateUTC")]
        public DateTimeOffset? LatestManifestDateUTC { get; set; }
        [JsonPropertyName("RemovedUTC")]
        public DateTimeOffset? RemovedUTC { get; set; }
        [JsonPropertyName("DisplayName")]
        public string DisplayName { get; set; }
        [JsonPropertyName("DisplayIcon")]
        public string DisplayIcon { get; set; }
        [JsonPropertyName("InVersions")]
        public HashSet<Guid> InVersions { get; set; } = [];
        [JsonPropertyName("JSONContent")]
        public string JSONContent { get; set; }
        [JsonIgnore]
        public bool IsDirty { get; set; } = false;
    }

}
