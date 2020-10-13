using Amazon.SQS;
using Amazon.SQS.Model;
using CloudPortAPI.Config;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    public class AwsMessageQueueService : IMessageQueueService
    {
        private AwsMessageQueueClientSettings _settings;
        private AWSSDKCredentials _cred;
        public AwsMessageQueueService(AWSSDKCredentials cred, AwsMessageQueueClientSettings settings)
        {
            _settings = settings;
            _cred = cred;
        }

        public async Task Receive()
        {
            AmazonSQSConfig amazonSQSConfig = new AmazonSQSConfig();

            amazonSQSConfig.ServiceURL = _settings.ServiceURL;
            AmazonSQSClient amazonSQSClient = new AmazonSQSClient(_cred.AwsAccessKeyId,_cred.AwsSecretAccessKey, amazonSQSConfig);

            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(_settings.QueueUrl);
            receiveMessageRequest.MaxNumberOfMessages = 10;

            List<Message> messages = new List<Message>();

            while (true)
            {
                var msgs = await amazonSQSClient.ReceiveMessageAsync(receiveMessageRequest);
                if(msgs.Messages.Count == 0)
                {
                    break;
                }
                else
                {
                    messages.AddRange(msgs.Messages);
                }
            }
            //Return actual messages
            //return messages;
        }

        public async Task<int> Send(string[] messages)
        {
            int result = 0;
            AmazonSQSConfig amazonSQSConfig = new AmazonSQSConfig();

            amazonSQSConfig.ServiceURL = _settings.ServiceURL;
            AmazonSQSClient amazonSQSClient = new AmazonSQSClient(_cred.AwsAccessKeyId,_cred.AwsSecretAccessKey,  amazonSQSConfig);

            foreach (var message in messages)
            {
                SendMessageResponse sendMessageResponse = await amazonSQSClient.SendMessageAsync(_settings.QueueUrl, message);
            }
            
            return result;
        }
    }
}
