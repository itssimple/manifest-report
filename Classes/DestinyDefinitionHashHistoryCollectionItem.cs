using System.Data;

namespace Manifest.Report.Classes
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
            DiscoveredUTC = row.Field<DateTime>("DiscoveredUTC");
            JSONContent = row.Field<string>("JSONContent");
            JSONContent = row.Field<string>("JSONContent");
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
    }
}
