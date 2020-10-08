using System.Collections.Generic;
using System.Data;

namespace CloudPortAPI.Services
{
    public interface IDatabaseService
    {
        int Add<T>(T obj) where T : class;
        int Add<T>(T[] list) where T : class;
        int Update<T>(T obj) where T : class;
        int Update<T>(T[] list) where T : class;
        int Remove<T>(T obj) where T : class;
        int Remove<T>(T[] list) where T : class;
        IEnumerable<T> Get<T>(T obj) where T : class;
        void Join<TP, T>(ref TP parent, T child) where T: class;
        void Join<TP, T>(ref IEnumerable<TP> parentList, T child) where T : class;
        DataTable CustomSqlQuery(string query);

        DataTable GetSqlSchema(string collection);
    }
}
