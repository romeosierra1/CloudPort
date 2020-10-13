using CloudPortAPI.Models;
using System.Net.Mail;
using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    public interface IEmailService
    {
        Task<int> Send(EmailModel[] emails);
        Task<int> Send(MailMessage[] emails);
    }
}
