using CloudPortAPI.Config;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    public class DatabaseService : IDatabaseService
    {
        private SqlDatabaseService _sqlDatabaseContext;
        private DocumentDatabaseService _noSqlDatabaseContext;
        private ColumnarDatabaseService _columnarDatabaseContext;

        public static int Offset = 999;

        public DatabaseService(SqlDatabaseSettings sqlDatabaseSettings,
            MongoDatabaseSettings mongoDatabaseSettings,
            ColumnarDatabaseSettings columnarDatabaseSettings)
        {
            if (sqlDatabaseSettings.ConnectionString != null)
            {
                _sqlDatabaseContext = new SqlDatabaseService(sqlDatabaseSettings);
            }

            if (mongoDatabaseSettings.ConnectionString != null && mongoDatabaseSettings.DatabaseName != null)
            {
                _noSqlDatabaseContext = new DocumentDatabaseService(mongoDatabaseSettings);
            }

            if (columnarDatabaseSettings.ContactPoint != null && columnarDatabaseSettings.KeySpace != null
                //&& columnarDatabaseSettings.Username != null && columnarDatabaseSettings.Password != null
                && columnarDatabaseSettings.Port != 0)
            {
                _columnarDatabaseContext = new ColumnarDatabaseService(columnarDatabaseSettings);
            }
        }

        public async Task<int> Add<T>(T obj) where T : class
        {
            int result = 0;

            var type = obj.GetType();

            if (type.BaseType.Name == "TSqlModel")
            {
                await _sqlDatabaseContext.Add(obj);
            }
            else if (type.BaseType.Name == "TMongoModel")
            {
                await _noSqlDatabaseContext.Add(obj);
            }
            else if (type.BaseType.Name == "TColumnarModel")
            {
                await _columnarDatabaseContext.Add(obj);
            }

            return result;
        }

        public async Task<int> Add<T>(T[] list) where T : class
        {
            int result = 0;

            if (list.Count() > 0)
            {
                var type = list.FirstOrDefault().GetType();

                if (type.BaseType.Name == "TSqlModel")
                {
                    await _sqlDatabaseContext.Add(list);
                }
                else if (type.BaseType.Name == "TMongoModel")
                {
                    await _noSqlDatabaseContext.Add(list);
                }
                else if (type.BaseType.Name == "TColumnarModel")
                {
                    await _columnarDatabaseContext.Add(list);
                }
            }

            return result;
        }

        public async Task<IEnumerable<T>> Get<T>(T obj) where T : class
        {
            var type = obj.GetType();

            IEnumerable<T> list = new List<T>();

            if (type.BaseType.Name == "TSqlModel")
            {
                list = await _sqlDatabaseContext.Get(obj);
            }
            else if (type.BaseType.Name == "TMongoModel")
            {
                list = await _noSqlDatabaseContext.Get(obj);
            }
            else if (type.BaseType.Name == "TColumnarModel")
            {
                list = await _columnarDatabaseContext.Get(obj);
            }

            return list;
        }

        public void Join<TP, T>(ref TP parent, T child) where T : class
        {
            var type = parent.GetType();

            if (type.BaseType.Name == "TSqlModel")
            {
                _sqlDatabaseContext.Join(ref parent, child);
            }
            else if (type.BaseType.Name == "TMongoModel")
            {
                _noSqlDatabaseContext.Join(ref parent, child);
            }
            else if (type.BaseType.Name == "TColumnarModel")
            {
                _columnarDatabaseContext.Join(ref parent, child);
            }
        }

        public void Join<TP, T>(ref IEnumerable<TP> parentList, T child) where T : class
        {
            if (parentList.Count() > 0)
            {
                var type = parentList.FirstOrDefault().GetType();

                if (type.BaseType.Name == "TSqlModel")
                {
                    _sqlDatabaseContext.Join(ref parentList, child);
                }
                else if (type.BaseType.Name == "TMongoModel")
                {
                    _noSqlDatabaseContext.Join(ref parentList, child);
                }
                else if (type.BaseType.Name == "TColumnarModel")
                {
                    _columnarDatabaseContext.Join(ref parentList, child);
                }
            }
        }

        public async Task<int> Remove<T>(T obj) where T : class
        {
            int result = 0;

            var type = obj.GetType();

            if (type.BaseType.Name == "TSqlModel")
            {
                await _sqlDatabaseContext.Remove(obj);
            }
            else if (type.BaseType.Name == "TMongoModel")
            {
                await _noSqlDatabaseContext.Remove(obj);
            }
            else if (type.BaseType.Name == "TColumnarModel")
            {
                await _columnarDatabaseContext.Remove(obj);
            }

            return result;
        }

        public async Task<int> Remove<T>(T[] list) where T : class
        {
            int result = 0;

            if (list.Count() > 0)
            {
                var type = list.FirstOrDefault().GetType();

                if (type.BaseType.Name == "TSqlModel")
                {
                    await _sqlDatabaseContext.Remove(list);
                }
                else if (type.BaseType.Name == "TMongoModel")
                {
                    await _noSqlDatabaseContext.Remove(list);
                }
                else if (type.BaseType.Name == "TColumnarModel")
                {
                    await _columnarDatabaseContext.Remove(list);
                }
            }

            return result;
        }

        public async Task<int> Update<T>(T obj) where T : class
        {
            int result = 0;

            var type = obj.GetType();

            if (type.BaseType.Name == "TSqlModel")
            {
                await _sqlDatabaseContext.Update(obj);
            }
            else if (type.BaseType.Name == "TMongoModel")
            {
                await _noSqlDatabaseContext.Update(obj);
            }
            else if (type.BaseType.Name == "TColumnarModel")
            {
                await _columnarDatabaseContext.Update(obj);
            }

            return result;
        }

        public async Task<int> Update<T>(T[] list) where T : class
        {
            int result = 0;

            if (list.Count() > 0)
            {
                var type = list.FirstOrDefault().GetType();

                if (type.BaseType.Name == "TSqlModel")
                {
                    await _sqlDatabaseContext.Update(list);
                }
                else if (type.BaseType.Name == "TMongoModel")
                {
                    await _noSqlDatabaseContext.Update(list);
                }
                else if (type.BaseType.Name == "TColumnarModel")
                {
                    await _columnarDatabaseContext.Update(list);
                }
            }

            return result;
        }

        public DataTable CustomSqlQuery(string query)
        {
            return _sqlDatabaseContext.CustomSqlQuery(query);
        }

        public DataTable GetSqlSchema(string collection)
        {
            return _sqlDatabaseContext.GetSchema(collection);
        }
    }
}
