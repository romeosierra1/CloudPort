using Amazon.SQS;
using Amazon.SQS.Model;
using CloudPortAPI.Config;

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

        public void Receive()
        {
            AmazonSQSConfig amazonSQSConfig = new AmazonSQSConfig();

            amazonSQSConfig.ServiceURL = _settings.ServiceURL;
            AmazonSQSClient amazonSQSClient = new AmazonSQSClient(_cred.AwsAccessKeyId,_cred.AwsSecretAccessKey, amazonSQSConfig);

            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(_settings.QueueUrl);
            receiveMessageRequest.MaxNumberOfMessages = 10;

            while (true)
            {
                var messages = amazonSQSClient.ReceiveMessageAsync(receiveMessageRequest).Result.Messages;
                if(messages.Count == 0)
                {
                    break;
                }
            }
            //Return actual messages
        }

        public int Send(string[] messages)
        {
            int result = 0;
            AmazonSQSConfig amazonSQSConfig = new AmazonSQSConfig();

            amazonSQSConfig.ServiceURL = _settings.ServiceURL;
            AmazonSQSClient amazonSQSClient = new AmazonSQSClient(_cred.AwsAccessKeyId,_cred.AwsSecretAccessKey,  amazonSQSConfig);

            foreach (var message in messages)
            {
                SendMessageResponse sendMessageResponse = amazonSQSClient.SendMessageAsync(_settings.QueueUrl, message).Result;
            }
            
            return result;
        }
    }
}
