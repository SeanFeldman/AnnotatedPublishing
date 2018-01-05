using NServiceBus.Pipeline;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JobScheduler.Messages;
using NServiceBus;
using NServiceBus.Extensibility;
using NServiceBus.Features;
using NServiceBus.Unicast.Subscriptions;
using NServiceBus.Unicast.Subscriptions.MessageDrivenSubscriptions;

namespace JobScheduler
{
    class ExtractJobStatusMessageIdBehavior : Behavior<IOutgoingLogicalMessageContext>
    {
        public override Task Invoke(IOutgoingLogicalMessageContext context, Func<Task> next)
        {
            if (context.Message.Instance is JobStatusMessage jobStatusMsg)
            {
                var annotatedType = $"JobScheduler.AnnotatedMessages.MasterJobId{jobStatusMsg.MasterJobId}Happened, JobScheduler.AnnotatedMessages, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null";
                context.Extensions.Set("JobStatusMessage.MasterJobId.EventType", annotatedType);
            }

            return next();
        }
    }

    class AnnotatePublishedMessageBehavior : Behavior<IOutgoingPhysicalMessageContext>
    {
        public override Task Invoke(IOutgoingPhysicalMessageContext context, Func<Task> next)
        {
            if (context.Extensions.TryGet("JobStatusMessage.MasterJobId.EventType", out string extraTypeName))
            {
                var existingTypes = context.Headers[Headers.EnclosedMessageTypes];
                var newTypes = $"{extraTypeName};{existingTypes}";
                context.Headers[Headers.EnclosedMessageTypes] = newTypes;
            }
            return next();
        }
    }

    class RegisterBehavior : INeedInitialization
    {
        public void Customize(EndpointConfiguration configuration)
        {
            configuration.Pipeline.Register(new ExtractJobStatusMessageIdBehavior(),
                "Extracts MasterJobId from JobStatusMessage and determines an annotated type to publish for it.");

            configuration.Pipeline.Register(new AnnotatePublishedMessageBehavior(),
                "Annotates JobStatusMessage events with a type based on the MasterJobId.");

            configuration.RegisterComponents(reg =>
            {
                reg.ConfigureComponent<ISubscriptionStorage>(() => new AlternateSubscriptionStorage(), DependencyLifecycle.SingleInstance);
            });

            configuration.DisableFeature<InMemorySubscriptionPersistence>();
        }
    }

    class AlternateSubscriptionStorage : ISubscriptionStorage
    {
        public Task Subscribe(Subscriber subscriber, MessageType messageType, ContextBag context)
        {
            // Equivalent SQL: https://github.com/Particular/NServiceBus.Persistence.Sql/blob/develop/src/SqlPersistence/Subscription/SqlDialect_MsSqlServer.cs#L20-L40
            var dict = storage.GetOrAdd(messageType, type => new ConcurrentDictionary<string, Subscriber>(StringComparer.OrdinalIgnoreCase));

            dict.AddOrUpdate(subscriber.TransportAddress, _ => subscriber, (_, __) => subscriber);
            return Task.CompletedTask;
        }

        public Task Unsubscribe(Subscriber subscriber, MessageType messageType, ContextBag context)
        {
            // Equivalent SQL: https://github.com/Particular/NServiceBus.Persistence.Sql/blob/develop/src/SqlPersistence/Subscription/SqlDialect_MsSqlServer.cs#L46-L50
            if (storage.TryGetValue(messageType, out var dict))
            {
                dict.TryRemove(subscriber.TransportAddress, out var _);
            }
            return Task.CompletedTask;
        }

        public Task<IEnumerable<Subscriber>> GetSubscriberAddressesForMessage(IEnumerable<MessageType> messageTypes, ContextBag context)
        {
            var result = new HashSet<Subscriber>();

            if (context is IOutgoingPublishContext publishContext)
            {
                if (publishContext.Message.Instance is JobStatusMessage jobStatusMsg)
                {
                    var annotatedType = $"JobScheduler.AnnotatedMessages.MasterJobId{jobStatusMsg.MasterJobId}Happened, JobScheduler.AnnotatedMessages, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null";
                    var newMessageType = new MessageType(annotatedType);

                    messageTypes = messageTypes.Union(new [] { newMessageType });
                }
            }

            // Equivalent SQL: https://github.com/Particular/NServiceBus.Persistence.Sql/blob/develop/src/SqlPersistence/Subscription/SqlDialect_MsSqlServer.cs#L55-L73

            foreach (var m in messageTypes)
            {
                if (storage.TryGetValue(m, out var list))
                {
                    result.UnionWith(list.Values);
                }
            }
            return Task.FromResult((IEnumerable<Subscriber>)result);
        }

        ConcurrentDictionary<MessageType, ConcurrentDictionary<string, Subscriber>> storage = new ConcurrentDictionary<MessageType, ConcurrentDictionary<string, Subscriber>>();
    }
}
