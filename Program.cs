using Amazon;
using Amazon.S3;
using Hangfire;
using Hangfire.Console;
using Hangfire.Redis.StackExchange;
using Manifest.Report;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddJsonFile(builder.Configuration["ConfigFile"] ?? "manifest.report.environment.json");

builder.Services.AddHttpClient();
builder.Services.AddHangfire(config =>
{
    config
        .UseSimpleAssemblyNameTypeSerializer()
        .UseRecommendedSerializerSettings()
        .UseRedisStorage(SharedSettings.RedisClient, new RedisStorageOptions
        {
            Db = 8
        })
        .UseConsole();
});

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

app.UseRouting();

app.UseAuthorization();

app.MapRazorPages();

app.Run();
