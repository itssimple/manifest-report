using System.Data;

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

        public long HistoryId { get; private set; }
        public string Definition { get; set; }
        public long Hash { get; set; }
        public Guid ManifestVersion { get; set; }
        public DateTimeOffset DiscoveredUTC { get; set; }
        public string JSONContent { get; set; }
        public string JSONDiff { get; set; }
        public DefinitionHashChangeState State { get; set; }

        public bool IsDirty { get; set; } = false;
    }

    public enum DefinitionHashChangeState
    {
        Added,
        Modified,
        Redacted,
        Unredacted,
        Deleted
    }
}
