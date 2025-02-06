namespace Manifest.Report.Classes
{
    public class ManifestInfo
    {
        public Guid VersionId { get; set; }
        public string Version { get; set; }
        public string ManifestJsonPath { get; set; }
        public string EnhancedManifestJsonPath { get; set; }
        public DateTimeOffset DiscoverDate_UTC { get; set; }
        public DateTimeOffset ManifestDate_UTC { get; set; }
        public List<FileDiff> DiffFiles { get; set; } = new List<FileDiff>();
    }
}
