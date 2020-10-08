using System.Net;
using System.Net.Mail;
using CloudPortAPI.Config;
using CloudPortAPI.Models;

namespace CloudPortAPI.Services
{
    public class EmailService : IEmailService
    {
        private EmailClientSettings _settings;
        public EmailService(EmailClientSettings settings)
        {
            _settings = settings;
        }
        public int Send(EmailModel[] emails)
        {
            int result = 0;

            SmtpClient client = new SmtpClient();
            client.Host = _settings.Host;
            client.Port = _settings.Port;
            client.EnableSsl = true;
            client.Credentials = new NetworkCredential(_settings.Username, _settings.Password);

            foreach (var email in emails)
            {
                MailMessage mailMessage = new MailMessage();
                mailMessage.To.Add(string.Join(",", email.To));
                if (email.Cc != null)
                mailMessage.CC.Add(string.Join(",", email.Cc));
                if(email.Bcc != null)
                mailMessage.Bcc.Add(string.Join(",", email.Bcc));
                mailMessage.From = new MailAddress(email.From);
                mailMessage.Subject = email.Subject;
                mailMessage.Body = email.Body;
                mailMessage.IsBodyHtml = email.IsBodyHtml;

                client.Send(mailMessage);
            }

            return result;
        }

        public int Send(MailMessage[] emails)
        {
            int result = 0;
            
            SmtpClient client = new SmtpClient();
            client.Host = _settings.Host;
            client.Port = _settings.Port;
            client.EnableSsl = true;
            client.Credentials = new NetworkCredential(_settings.Username, _settings.Password);

            foreach (var email in emails)
            {
                client.Send(email);
            }

            return result;
        }
    }
}
