namespace Manifest.Report.Classes
{
    public class DestinyDefinitionChanges
    {
        public HashSet<long> Added { get; set; } = new HashSet<long>();
        public HashSet<long> Modified { get; set; } = new HashSet<long>();
        public HashSet<long> Unclassified { get; set; } = new HashSet<long>();
        public HashSet<long> Reclassified { get; set; } = new HashSet<long>();
        public HashSet<long> Removed { get; set; } = new HashSet<long>();
        public int Changes { get; set; } = 0;
    }
}
