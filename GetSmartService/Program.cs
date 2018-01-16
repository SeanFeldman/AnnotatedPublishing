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

            // Hack to make sure we count valid messages before IEvent handler counting total messages 
            cfg.ExecuteTheseHandlersFirst(typeof(OnMasterJobId86), typeof(OnMasterJobId99));

            var endpoint = await Endpoint.Start(cfg);

            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();

            await endpoint.Stop();
        }
    }

    public class OnMasterJobId86 : IHandleMessages<MasterJobId86Happened>
    {
        public static int Count;

        public Task Handle(MasterJobId86Happened message, IMessageHandlerContext context)
        {
            Interlocked.Increment(ref Count);
            return Task.CompletedTask;
        }
    }

    public class OnMasterJobId99 : IHandleMessages<MasterJobId99Happened>
    {
        public static int Count;

        public Task Handle(MasterJobId99Happened message, IMessageHandlerContext context)
        {
            Interlocked.Increment(ref Count);
            return Task.CompletedTask;
        }
    }





    public class JustCountingTotalNumberOfMessages : IHandleMessages<IEvent>
    {
        private static int total;

        public Task Handle(IEvent message, IMessageHandlerContext context)
        {
            Interlocked.Increment(ref total);
            var dontCare = total - OnMasterJobId86.Count - OnMasterJobId99.Count;
            Console.WriteLine($"Received {OnMasterJobId86.Count} MasterJobId86, {OnMasterJobId99.Count} MasterJobId99, {dontCare} wasteful messages we don't care about, {total} total");
            return Task.CompletedTask;
        }
    }
}
