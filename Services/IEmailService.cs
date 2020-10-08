using CloudPortAPI.Models;
using System.Net.Mail;

namespace CloudPortAPI.Services
{
    public interface IEmailService
    {
        int Send(EmailModel[] emails);
        int Send(MailMessage[] emails);
    }
}
