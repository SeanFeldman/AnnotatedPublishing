using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JobScheduler.Messages;
using NServiceBus;
using NServiceBus.Pipeline;

namespace JobScheduler
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.Title = "JobScheduler";

            var cfg = new EndpointConfiguration("JobScheduler");

            var transport = cfg.UseTransport<MsmqTransport>();
            var routing = transport.Routing();

            var persistence = cfg.UsePersistence<InMemoryPersistence>();

            cfg.SendFailedMessagesTo("error");

            var endpoint = await Endpoint.Start(cfg);

            Console.WriteLine("Press (P) to publish 1,000 JobStatusMessages, any other key to exit.");
            while (true)
            {
                var key = Console.ReadKey(true);
                if (key.Key == ConsoleKey.P)
                {
                    for (var i = 0; i < 1/*000*/; i++)
                    {
                        var evt = new JobStatusMessage
                        {
                            MasterJobId = 99//random.Next(0, 100)
                        };

                        await endpoint.Publish(evt);
                    }
                }
                else
                {
                    break;
                }
            }

            await endpoint.Stop();
        }

        private static Random random = new Random();
    }
}
