namespace Manifest.Report.Classes
{
    public class FileDiff
    {
        public string FileName { get; set; }
        public int Added { get; set; }
        public int Modified { get; set; }
        public int Unclassified { get; set; }
        public int Reclassified { get; set; }
        public int Removed { get; set; }
        public FileStatus FileStatus { get; set; }
    }

    public enum FileStatus
    {
        Added,
        Modified,
        Removed
    }
}
