namespace CloudPortAPI.Services
{
    public interface IMessageQueueService
    {
        int Send(string[] messages);
        void Receive();
    }
}
