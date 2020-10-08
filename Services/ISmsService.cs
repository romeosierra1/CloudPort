namespace CloudPortAPI.Services
{
    public interface ISmsService
    {
        int Send(string phoneNumber, string message);
    }
}
