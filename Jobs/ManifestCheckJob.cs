using Amazon.S3;
using Hangfire.Server;

namespace Manifest.Report.Jobs
{
    public class ManifestCheckJob(ManifestVersionArchiver archiver)
    {
        public async Task CheckManifest(PerformContext context)
        {
            await archiver.CheckForNewManifest(context);
        }
    }
}
