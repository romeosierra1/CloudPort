using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    public interface IDatabaseService
    {
        Task<int> Add<T>(T obj) where T : class;
        Task<int> Add<T>(T[] list) where T : class;
        Task<int> Update<T>(T obj) where T : class;
        Task<int> Update<T>(T[] list) where T : class;
        Task<int> Remove<T>(T obj) where T : class;
        Task<int> Remove<T>(T[] list) where T : class;
        Task<IEnumerable<T>> Get<T>(T obj) where T : class;
        void Join<TP, T>(ref TP parent, T child) where T: class;
        void Join<TP, T>(ref IEnumerable<TP> parentList, T child) where T : class;
        DataTable CustomSqlQuery(string query);

        DataTable GetSqlSchema(string collection);
    }
}
