using Amazon.S3;
using Hangfire.Server;

namespace Manifest.Report.Jobs
{
    public class ManifestCheckJob(ILogger<ManifestVersionArchiver> logger, IHttpClientFactory httpClientFactory, AmazonS3Client s3Client)
    {
        public async Task CheckManifest(PerformContext context)
        {
            var _manifestVersionArchiver = new ManifestVersionArchiver(logger, httpClientFactory, s3Client);
            await _manifestVersionArchiver.CheckForNewManifest(context);
        }
    }
}
