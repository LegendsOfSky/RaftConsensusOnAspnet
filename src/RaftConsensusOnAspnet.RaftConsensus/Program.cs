
namespace RaftConsensusOnAspnet.RaftConsensus;

public class Program
{
    public static void Main(string[] args)
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
        builder.Services.AddAuthorization();        // Add services to the container.
        builder.Services.AddOpenApi();              // Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
        builder.Services.AddEndpointsApiExplorer(); // Add Swagger services
        builder.Services.AddSwaggerGen();           // ^
        WebApplication app = builder.Build();

        // Configure the HTTP request pipeline.
        if (app.Environment.IsDevelopment())
        {
            app.MapOpenApi();
            app.UseSwagger();
            app.UseSwaggerUI();
        }
        app.UseHttpsRedirection();
        app.UseAuthorization();

        string[] summaries =
        [
            "Freezing" , "Bracing" , "Chilly" , "Cool" , "Mild" , "Warm" , "Balmy" , "Hot" , "Sweltering" , "Scorching" ,
        ];

        app.MapGet(
                "/weatherforecast" , (HttpContext httpContext) =>
                {
                    WeatherForecast[] forecast =
                    [
                        .. Enumerable.Range(1 , 5).Select(
                                index =>
                                    new WeatherForecast
                                    {
                                        Date = DateOnly.FromDateTime(DateTime.Now.AddDays(index)) ,
                                        TemperatureC = Random.Shared.Next(-20 , 55) ,
                                        Summary = summaries[Random.Shared.Next(summaries.Length)]
                                    }
                            ) ,
                    ];
                    return forecast;
                }
            ).WithName("GetWeatherForecast");

        app.Run();
    }
}
