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

    public class JobStatusHandler : IHandleMessages<JobStatusMessage>
    {
        public static int MessageReceived;

        public Task Handle(JobStatusMessage message, IMessageHandlerContext context)
        {
            Interlocked.Increment(ref MessageReceived);
            return Task.CompletedTask;
        }
    }

    public class OnMasterJobId42 : IHandleMessages<MasterJobId42Happened>
    {
        private static int messagesReceived;

        public Task Handle(MasterJobId42Happened message, IMessageHandlerContext context)
        {
            var received = Interlocked.Increment(ref messagesReceived);
            Console.WriteLine($"Received MasterJobId42Happened, {received} received of this type, {JobStatusHandler.MessageReceived} total messages so far...");
            return Task.CompletedTask;
        }
    }

    public class OnMasterJobId13 : IHandleMessages<MasterJobId7Happened>
    {
        private static int messagesReceived;

        public Task Handle(MasterJobId7Happened message, IMessageHandlerContext context)
        {
            var received = Interlocked.Increment(ref messagesReceived);
            Console.WriteLine($"Received MasterJobId7Happened, {received} received of this type, {JobStatusHandler.MessageReceived} total messages so far...");
            return Task.CompletedTask;
        }
    }
}
