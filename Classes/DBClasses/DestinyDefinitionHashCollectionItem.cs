using System.Data;
using System.Text.Json;

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
            InVersions = JsonSerializer.Deserialize<HashSet<Guid>>(row.Field<string>("InVersions")) ?? new HashSet<Guid>();
            JSONContent = row.Field<string>("JSONContent");
        }

        public long HashCollectionId { get; private set; }
        public string Definition { get; set; }
        public long Hash { get; set; }
        public DateTimeOffset? FirstDiscoveredUTC { get; set; }
        public DateTimeOffset? LatestManifestDateUTC { get; set; }
        public DateTimeOffset? RemovedUTC { get; set; }
        public string DisplayName { get; set; }
        public string DisplayIcon { get; set; }
        public HashSet<Guid> InVersions { get; set; } = new HashSet<Guid>();
        public string JSONContent { get; set; }

        public bool IsDirty { get; set; } = false;
    }

}
