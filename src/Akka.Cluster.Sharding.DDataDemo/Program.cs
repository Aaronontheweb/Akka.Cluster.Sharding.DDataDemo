using System;
using System.Threading.Tasks;
using Jaeger;
using Jaeger.Reporters;
using Jaeger.Samplers;
using Jaeger.Senders;
using Jaeger.Senders.Thrift;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTracing;
using Phobos.Tracing.Scopes;

namespace Petabridge.App
{
    public class Program
    {
        /// <summary>
        ///     Name of the <see cref="Environment" /> variable used to direct Phobos' Jaeger
        ///     output.
        ///     See https://github.com/jaegertracing/jaeger-client-csharp for details.
        /// </summary>
        public const string JaegerAgentHostEnvironmentVar = "JAEGER_AGENT_HOST";

        public const string JaegerEndpointEnvironmentVar = "JAEGER_ENDPOINT";

        public const string JaegerAgentPortEnvironmentVar = "JAEGER_AGENT_PORT";

        public const int DefaultJaegerAgentPort = 6832;
        
        public static void ConfigureJaegerTracing(IServiceCollection services)
        {
            static ISender BuildSender()
            {
                if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(JaegerEndpointEnvironmentVar)))
                {
                    if (!int.TryParse(Environment.GetEnvironmentVariable(JaegerAgentPortEnvironmentVar),
                        out var udpPort))
                        udpPort = DefaultJaegerAgentPort;
                    return new UdpSender(
                        Environment.GetEnvironmentVariable(JaegerAgentHostEnvironmentVar) ?? "localhost",
                        udpPort, 0);
                }

                return new HttpSender(Environment.GetEnvironmentVariable(JaegerEndpointEnvironmentVar));
            }

            services.AddSingleton<ITracer>(sp =>
            {
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();

                var builder = BuildSender();
                var logReporter = new LoggingReporter(loggerFactory);

                var remoteReporter = new RemoteReporter.Builder()
                    .WithLoggerFactory(loggerFactory) // optional, defaults to no logging
                    .WithMaxQueueSize(100) // optional, defaults to 100
                    .WithFlushInterval(TimeSpan.FromSeconds(1)) // optional, defaults to TimeSpan.FromSeconds(1)
                    .WithSender(builder) // optional, defaults to UdpSender("localhost", 6831, 0)
                    .Build();

                var sampler = new ConstSampler(true); // keep sampling disabled

                // name the service after the executing assembly
                var tracer = new Tracer.Builder(typeof(AkkaService).Assembly.GetName().Name)
                    .WithReporter(new CompositeReporter(remoteReporter, logReporter))
                    .WithSampler(sampler)
                    .WithScopeManager(
                        new ActorScopeManager()); // IMPORTANT: ActorScopeManager needed to properly correlate trace inside Akka.NET

                return tracer.Build();
            });
        }
        
        public static async Task Main(string[] args)
        {
            var host = new HostBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddLogging();

                    ConfigureJaegerTracing(services);                    
                    services.AddHostedService<AkkaService>(); // runs Akka.NET
 
                })
                .ConfigureLogging((hostContext, configLogging) =>
                {
                    configLogging.AddConsole();W
                })
                .UseConsoleLifetime()
                .Build();

            await host.RunAsync();
        }
    }
   
}
