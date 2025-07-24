using Manifest.Report.Classes;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Manifest.Report
{
    [Route("api/[controller]")]
    [ApiController]
    public class SearchController(MSSQLDB db) : ControllerBase
    {
    }
}
