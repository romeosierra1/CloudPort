using CloudPortAPI.Config;
using System;
using System.IO;
using System.Net;
using System.Text;

namespace CloudPortAPI.Services
{
    public class SmsService : ISmsService
    {
        private SmsClientSettings _settings;

        public SmsService(SmsClientSettings settings)
        {
            _settings = settings;
        }

        public int Send(string phoneNumber, string message)
        {
            int result = 0;
            WebRequest request = null;
            HttpWebResponse response = null;
            try
            {
                string url = $"http://enterprise.smsgupshup.com/GatewayAPI/rest?method=sendMessage&send_to={phoneNumber}&msg={WebUtility.UrlEncode(message)}&userid={_settings.Username}&password={_settings.Password}&v=1.1msg_type=TEXT&auth_scheme=PLAIN";
                request = WebRequest.Create(url);

                response = (HttpWebResponse)request.GetResponse();
                Stream stream = response.GetResponseStream();
                Encoding ec = System.Text.Encoding.GetEncoding("utf-8");
                StreamReader reader = new
                System.IO.StreamReader(stream, ec);
                string r = reader.ReadToEnd();
                Console.WriteLine(r);
                reader.Close();
                stream.Close();
            }
            catch (Exception exp)
            {
                Console.WriteLine(exp.ToString());
            }
            finally
            {
                if (response != null)
                    response.Close();
            }
            return result;
        }
    }
}
