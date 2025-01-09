using Manifest.Report;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace manifest_report.Pages
{
    public class IndexModel : PageModel
    {
        private readonly ILogger<IndexModel> _logger;
        private readonly ManifestVersionArchiver _manifestVersionArchiver;

        public IndexModel(ILogger<IndexModel> logger, ManifestVersionArchiver manifestVersionArchiver)
        {
            _logger = logger;
            _manifestVersionArchiver = manifestVersionArchiver;
        }

        public async Task OnGet()
        {
            await _manifestVersionArchiver.CheckForNewManifest();
        }
    }
}
