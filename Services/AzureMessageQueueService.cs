using CloudPortAPI.Config;
using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    public class AzureMessageQueueService : IMessageQueueService
    {
        private AzureMessageQueueClientSettings _settings;

        public AzureMessageQueueService(AzureMessageQueueClientSettings settings)
        {
            _settings = settings;
        }
        public async Task Receive()
        {
            IQueueClient queueClient = new QueueClient(_settings.ConnectionString, _settings.QueueName);

            var messageHandlerOptions = new MessageHandlerOptions((exceptionReceivedEventArgs) => {
                Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
                var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
                Console.WriteLine("Exception context for troubleshooting:");
                Console.WriteLine($"- Endpoint: {context.Endpoint}");
                Console.WriteLine($"- Entity Path: {context.EntityPath}");
                Console.WriteLine($"- Executing Action: {context.Action}");
                return Task.CompletedTask; })
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };
            
             queueClient.RegisterMessageHandler(async (message, token) => { await queueClient.CompleteAsync(message.SystemProperties.LockToken); }, messageHandlerOptions);

            //Return actual messages

        }
        
        public async Task<int> Send(string[] messages)
        {
            int result = 0;

            IQueueClient queueClient = new QueueClient(_settings.ConnectionString, _settings.QueueName);

            foreach (var message in messages)
            {
                var msg = new Message(Encoding.UTF8.GetBytes(message));
                await queueClient.SendAsync(msg);
            }            

            return result;
        }        
    }
}
