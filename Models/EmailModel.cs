namespace CloudPortAPI.Models
{
    public class EmailModel
    {
        public string[] To { get; set; }
        public string From { get; set; }
        public string[] Cc { get; set; }
        public string[] Bcc { get; set; }
        public string Subject { get; set; }
        public string Body { get; set; }
        public bool IsBodyHtml { get; set; }
    }
}
