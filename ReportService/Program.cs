using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JobScheduler.AnnotatedMessages;
using JobScheduler.Messages;
using NServiceBus;

namespace ReportService
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.Title = "ReportService";

            var cfg = new EndpointConfiguration("ReportService");

            var transport = cfg.UseTransport<MsmqTransport>();
            var routing = transport.Routing();

            routing.RegisterPublisher(typeof(JobStatusMessage), "JobScheduler");

            var persistence = cfg.UsePersistence<InMemoryPersistence>();

            cfg.SendFailedMessagesTo("error");

            var endpoint = await Endpoint.Start(cfg);

            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();

            await endpoint.Stop();
        }
    }

    public class LegacyJobStatusHandlerSubscribesToEverything : IHandleMessages<JobStatusMessage>
    {
        private static int dontCare;
        private static int job7Received;
        private static int job42Received;

        public Task Handle(JobStatusMessage message, IMessageHandlerContext context)
        {
            if (message.MasterJobId == 42)
            {
                Interlocked.Increment(ref job42Received);
            }
            else if (message.MasterJobId == 7)
            {
                Interlocked.Increment(ref job7Received);
            }
            else
            {
                Interlocked.Increment(ref dontCare);
            }

            Console.WriteLine($"Received {job7Received} MasterJobId7, {job42Received} MasterJobId42, {dontCare} wasteful messages we don't care about, {dontCare + job7Received + job42Received} total");
            return Task.CompletedTask;
        }
    }
}
