using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CloudPortAPI.Services
{
    public class GoogleMessageQueueService : IMessageQueueService
    {
        private string _projectId;
        private string _topicId;
        private string _subscriptionId;

        private PublisherServiceApiClient publisher;
        private SubscriberServiceApiClient subscriber;


        public GoogleMessageQueueService(string projectId, string topicId, string subscriptionId)
        {
            _projectId = projectId;
            _topicId = topicId;
            _subscriptionId = subscriptionId;
            publisher = PublisherServiceApiClient.Create();
            subscriber = SubscriberServiceApiClient.Create();
        }

        public void Receive()
        { 
            TopicName topicName = new TopicName(_projectId, _topicId);
            //publisher.CreateTopic(topicName);
            publisher.GetTopic(topicName);

            SubscriptionName subscriptionName = new SubscriptionName(_projectId, _subscriptionId);
            //subscriber.CreateSubscription(subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 60);
            subscriber.GetSubscription(subscriptionName);

            PullResponse response = subscriber.Pull(subscriptionName, false, 1000);
            foreach (ReceivedMessage received in response.ReceivedMessages)
            {
                PubsubMessage msg = received.Message;
                //Console.WriteLine($"Received message {msg.MessageId} published at {msg.PublishTime.ToDateTime()}");
                //Console.WriteLine($"Text: '{msg.Data.ToStringUtf8()}'");
            }

            // Acknowledge that we've received the messages. If we don't do this within 60 seconds (as specified
            // when we created the subscription) we'll receive the messages again when we next pull.
            subscriber.Acknowledge(subscriptionName, response.ReceivedMessages.Select(m => m.AckId));

            // Tidy up by deleting the subscription and the topic.
            //subscriber.DeleteSubscription(subscriptionName);
            //publisher.DeleteTopic(topicName);
        }

        public int Send(string[] messages)
        {
            // First create a topic.
            PublisherServiceApiClient publisher = PublisherServiceApiClient.Create();
            TopicName topicName = new TopicName(_projectId, _topicId);
            //publisher.CreateTopic(topicName);
            publisher.GetTopic(topicName);

            // Subscribe to the topic.
            SubscriberServiceApiClient subscriber = SubscriberServiceApiClient.Create();
            SubscriptionName subscriptionName = new SubscriptionName(_projectId, _subscriptionId);
            //subscriber.CreateSubscription(subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 60);
            subscriber.GetSubscription(subscriptionName);

            var msgs = messages.Select(msg =>
                new PubsubMessage
                {
                    // The data is any arbitrary ByteString. Here, we're using text.
                    Data = ByteString.CopyFromUtf8(msg),
                    // The attributes provide metadata in a string-to-string dictionary.
                    Attributes =
                            {
                                { "description", "Simple text message" }
                            }
                }
            );
            publisher.Publish(topicName, msgs);
            
            return messages.Length;
        }
    }
}
