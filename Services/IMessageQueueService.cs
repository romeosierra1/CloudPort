using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    public interface IMessageQueueService
    {
        Task<int> Send(string[] messages);
        Task Receive();
    }
}
