using Amazon.S3;

namespace Manifest.Report.Jobs
{
    public class ManifestCheckJob(ILogger<ManifestVersionArchiver> logger, HttpClient httpClient, AmazonS3Client s3Client)
    {
        public async Task CheckManifest()
        {
            var _manifestVersionArchiver = new ManifestVersionArchiver(logger, httpClient, s3Client);
            await _manifestVersionArchiver.CheckForNewManifest();
        }
    }
}
