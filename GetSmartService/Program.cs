using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JobScheduler.AnnotatedMessages;
using JobScheduler.Messages;
using NServiceBus;

namespace GetSmartService
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.Title = "GetSmartService";

            var cfg = new EndpointConfiguration("GetSmartService");

            var transport = cfg.UseTransport<MsmqTransport>();
            var routing = transport.Routing();

            // No routing for JobStatusMessage, so we don't get all those events we don't want
            routing.RegisterPublisher(typeof(MasterJobId86Happened), "JobScheduler");
            routing.RegisterPublisher(typeof(MasterJobId99Happened), "JobScheduler");

            var persistence = cfg.UsePersistence<InMemoryPersistence>();

            cfg.SendFailedMessagesTo("error");
            cfg.Recoverability()
                .Immediate(x => x.NumberOfRetries(0))
                .Delayed(x => x.NumberOfRetries(0));

            var endpoint = await Endpoint.Start(cfg);
            Console.WriteLine(typeof(MasterJobId86Happened).AssemblyQualifiedName);
            Console.WriteLine(typeof(MasterJobId99Happened).AssemblyQualifiedName);

            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();

            await endpoint.Stop();
        }
    }

    public class JobStatusHandler : IHandleMessages<IEvent>
    {
        public static int MessageReceived;

        public Task Handle(IEvent message, IMessageHandlerContext context)
        {
            Interlocked.Increment(ref MessageReceived);
            return Task.CompletedTask;
        }
    }

    public class OnMasterJobId86 : IHandleMessages<MasterJobId86Happened>
    {
        private static int messagesReceived;

        public Task Handle(MasterJobId86Happened message, IMessageHandlerContext context)
        {
            var received = Interlocked.Increment(ref messagesReceived);
            Console.WriteLine($"Received MasterJobId42Happened, {received} received of this type, {JobStatusHandler.MessageReceived} total IEvents so far...");
            return Task.CompletedTask;
        }
    }

    public class OnMasterJobId99 : IHandleMessages<MasterJobId99Happened>
    {
        private static int messagesReceived;

        public Task Handle(MasterJobId99Happened message, IMessageHandlerContext context)
        {
            var received = Interlocked.Increment(ref messagesReceived);
            Console.WriteLine($"Received MasterJobId7Happened, {received} received of this type, {JobStatusHandler.MessageReceived} total IEvents so far...");
            return Task.CompletedTask;
        }
    }
}
