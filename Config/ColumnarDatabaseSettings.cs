namespace CloudPortAPI.Config
{
    public class ColumnarDatabaseSettings
    {
        public string Cloud { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string ContactPoint { get; set; }
        public string KeySpace { get; set; }

        public int Port { get; set; }
    }
}
