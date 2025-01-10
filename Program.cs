using Amazon;
using Amazon.S3;
using Hangfire;
using Hangfire.Console;
using Hangfire.Dashboard.BasicAuthorization;
using Hangfire.Redis.StackExchange;
using Manifest.Report;
using Manifest.Report.Jobs;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddJsonFile(builder.Configuration["ConfigFile"] ?? "manifest.report.environment.json");

string hangfireUser = builder.Configuration["Hangfire:User"] ?? "local";
string hangfirePassword = builder.Configuration["Hangfire:Password"] ?? "host";

string bungieApiKey = builder.Configuration["Bungie:ApiKey"] ?? string.Empty;

var redisHost = builder.Configuration["Redis:Host"] ?? "127.0.0.1:6739";
var redis = ConnectionMultiplexer.Connect(redisHost);

builder.Services.AddHttpClient("Bungie", config =>
{
    config.DefaultRequestHeaders.Add("X-API-Key", bungieApiKey);
});

builder.Services.AddHangfire(config =>
{
    config
        .UseSimpleAssemblyNameTypeSerializer()
        .UseRecommendedSerializerSettings()
        .UseRedisStorage(redis, new RedisStorageOptions
        {
            Db = 8,
            Prefix = "manifest-report:"
        })
        .UseConsole();
});
builder.Services.AddHangfireServer();

builder.Services.AddScoped(config =>
{
    var accessKey = builder.Configuration["Minio:AccessKey"];
    var secretKey = builder.Configuration["Minio:SecretKey"];
    var s3Config = new AmazonS3Config
    {
        AuthenticationRegion = RegionEndpoint.EUWest1.SystemName,
        ServiceURL = builder.Configuration["Minio:ConnectionString"],
        ForcePathStyle = true
    };
    return new AmazonS3Client(accessKey, secretKey, s3Config);
});

builder.Services.AddScoped<ManifestVersionArchiver>();

builder.Services.AddRazorPages();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseHangfireDashboard("/hangfire", new DashboardOptions
{
    Authorization =
    [
        new BasicAuthAuthorizationFilter(new BasicAuthAuthorizationFilterOptions
        {
            SslRedirect = false,
            RequireSsl = false,
            LoginCaseSensitive = true,
            Users =
            [
                new BasicAuthAuthorizationUser
                {
                    Login = hangfireUser,
                    PasswordClear = hangfirePassword
                }
            ]
        })
    ]
});

app.UseRouting();

app.UseAuthorization();

app.MapRazorPages();

RecurringJob.AddOrUpdate<ManifestCheckJob>("manifest:checknew", x => x.CheckManifest(null), "*/5 * * * *");

app.Run();
