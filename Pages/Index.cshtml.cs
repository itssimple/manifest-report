using Manifest.Report;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace manifest_report.Pages
{
    public class IndexModel : PageModel
    {
        private readonly ILogger<IndexModel> _logger;

        public IndexModel(ILogger<IndexModel> logger)
        {
            _logger = logger;
        }

        public async Task OnGet()
        {
        }
    }
}
