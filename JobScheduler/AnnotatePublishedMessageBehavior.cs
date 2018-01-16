using NServiceBus.Pipeline;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JobScheduler.Messages;
using NServiceBus;
using NServiceBus.Extensibility;
using NServiceBus.Features;
using NServiceBus.ObjectBuilder;
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

    public class PublishMasterJobIdEventsFeature : Feature
    {
        public PublishMasterJobIdEventsFeature()
        {
            EnableByDefault();
            DependsOn<MessageDrivenSubscriptions>();
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            context.Pipeline.Register(new ExtractJobStatusMessageIdBehavior(),
                "Extracts MasterJobId from JobStatusMessage and determines an annotated type to publish for it.");

            context.Pipeline.Register(new AnnotatePublishedMessageBehavior(),
                "Annotates JobStatusMessage events with a type based on the MasterJobId.");

            context.RegisterStartupTask(builder => new Startup(context.Container, builder));
        }

        class Startup : FeatureStartupTask
        {
            private IConfigureComponents container;
            private IBuilder builder;

            public Startup(IConfigureComponents container, IBuilder builder)
            {
                this.container = container;
                this.builder = builder;
            }

            protected override Task OnStart(IMessageSession session)
            {
                var realStorage = builder.Build<ISubscriptionStorage>();
                container.RegisterSingleton<ISubscriptionStorage>(new SubscriptionStorageWrapper(realStorage));

                return Task.CompletedTask;
            }

            protected override Task OnStop(IMessageSession session)
            {
                return Task.CompletedTask;
            }
        }
    }

    class SubscriptionStorageWrapper : ISubscriptionStorage
    {
        private ISubscriptionStorage realStorage;

        public SubscriptionStorageWrapper(ISubscriptionStorage realStorage)
        {
            this.realStorage = realStorage;
        }

        public Task Subscribe(Subscriber subscriber, MessageType messageType, ContextBag context)
        {
            return realStorage.Subscribe(subscriber, messageType, context);
        }

        public Task Unsubscribe(Subscriber subscriber, MessageType messageType, ContextBag context)
        {
            return realStorage.Unsubscribe(subscriber, messageType, context);
        }

        public Task<IEnumerable<Subscriber>> GetSubscriberAddressesForMessage(IEnumerable<MessageType> messageTypes, ContextBag context)
        {
            if (context is IOutgoingPublishContext publishContext)
            {
                if (publishContext.Message.Instance is JobStatusMessage jobStatusMsg)
                {
                    var annotatedType = $"JobScheduler.AnnotatedMessages.MasterJobId{jobStatusMsg.MasterJobId}Happened, JobScheduler.AnnotatedMessages, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null";
                    var newMessageType = new MessageType(annotatedType);

                    messageTypes = messageTypes.Union(new[] { newMessageType });
                }
            }

            return realStorage.GetSubscriberAddressesForMessage(messageTypes, context);
        }
    }
}
