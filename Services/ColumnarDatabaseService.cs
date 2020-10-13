using Cassandra;
using CloudPortAPI.Config;
using Microsoft.Azure.Amqp.Framing;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    class ColumnarDatabaseService //: IDatabaseService
    {
        private ColumnarDatabaseSettings _settings;
        public ColumnarDatabaseService(ColumnarDatabaseSettings settings)
        {
            _settings = settings;
        }
        public async Task<int> Add<T>(T obj) where T : class
        {
            int result = 0;

            Type type = obj.GetType();

            if (type.IsClass)
            {
                string query = GenerateInsertQuery(type);
                await ExecuteStatement(query, obj);
            }

            return result;
        }

        public async Task<int> Add<T>(T[] list) where T : class
        {
            int result = 0;

            Type type = list.FirstOrDefault().GetType();

            if (type.IsClass)
            {
                string query = GenerateInsertQuery(type);
                await ExecuteStatement(query, list, "insert");
            }

            return result;
        }

        private string GenerateInsertQuery(Type type, string postFix = "")
        {
            string query = $"INSERT INTO {type.Name}";
            query += " (";
            foreach (var propertyInfo in type.GetProperties())
            {
                bool IsParameter = true;

                if (propertyInfo.PropertyType.IsClass)
                {
                    if (propertyInfo.PropertyType.Name.ToLower() != "string")
                    {
                        IsParameter = false;
                    }
                }

                if (IsParameter)
                {
                    query += propertyInfo.Name + ",";
                }
            }
            query = query.TrimEnd(',');
            query += ") ";
            query += "VALUES";
            query += " (";
            foreach (var propertyInfo in type.GetProperties())
            {
                bool IsParameter = true;

                if (propertyInfo.PropertyType.IsClass)
                {
                    if (propertyInfo.PropertyType.Name.ToLower() != "string")
                    {
                        IsParameter = false;
                    }
                }

                if (IsParameter)
                {
                    query += $":{propertyInfo.Name},";
                }

            }
            query = query.TrimEnd(',');
            query += ")";

            return query;
        }

        public async Task<IEnumerable<T>> Get<T>(T obj) where T : class
        {
            Type type = obj.GetType();

            List<T> result = new List<T>();

            if (type.IsClass)
            {
                string query = $"SELECT * FROM {type.Name}";

                var rowSet = await ExecuteStatement(query, obj);

                await Task.Run(() =>
                {
                    foreach (Row row in rowSet.GetRows())
                    {
                        var item = Activator.CreateInstance<T>();

                        foreach (CqlColumn column in rowSet.Columns)
                        {
                            PropertyInfo property = item.GetType().GetProperties().FirstOrDefault(p => p.Name == column.Name);

                            if (property != null && row.GetValue(column.Type, column.Name) != null)
                            {
                                property.SetValue(item, row.GetValue(column.Type, column.Name));
                            }
                        }

                        result.Add(item);
                    }
                });                
            }

            return result;
        }

        public async Task<int> Remove<T>(T obj) where T : class
        {
            int result = 0;

            Type type = obj.GetType();

            if (type.IsClass)
            {
                string query = GenerateDeleteQuery(type);

                await ExecuteStatement(query, obj);
            }

            return result;
        }

        public async Task<int> Remove<T>(T[] list) where T : class
        {
            int result = 0;

            Type type = list.FirstOrDefault().GetType();

            if (type.IsClass)
            {
                string query = GenerateDeleteQuery(type);

                await ExecuteStatement(query, list, "delete");
            }

            return result;
        }

        private string GenerateDeleteQuery(Type type, string postFix = "")
        {
            return $"DELETE FROM {type.Name} WHERE Id = :Id";
        }

        public async Task<int> Update<T>(T obj) where T : class
        {
            int result = 0;

            Type type = obj.GetType();

            if (type.IsClass)
            {
                string query = GenerateUpdateQuery(type);
                await ExecuteStatement(query, obj);
            }

            return result;
        }

        public async Task<int> Update<T>(T[] list) where T : class
        {
            int result = 0;

            Type type = list.FirstOrDefault().GetType();

            if(type.IsClass)
            {
                string query = GenerateUpdateQuery(type);
                await ExecuteStatement(query, list, "update");
            }

            return result;
        }

        private string GenerateUpdateQuery(Type type, string postFix ="")
        {
            string query = $"UPDATE {type.Name}";
            query += " SET ";
            foreach (var propertyInfo in type.GetProperties())
            {
                bool IsParameter = true;

                if (propertyInfo.PropertyType.IsClass)
                {
                    if (propertyInfo.PropertyType.Name.ToLower() != "string")
                    {
                        IsParameter = false;
                    }
                }

                if(propertyInfo.Name == "Id")
                {
                    IsParameter = false;
                }

                if (IsParameter)
                {
                    query += propertyInfo.Name + $" = :{propertyInfo.Name},";
                }
            }
            query = query.TrimEnd(',');
            query += " WHERE ";
            query += "Id = :Id";

            return query;
        }

        private async Task<RowSet> ExecuteStatement<T>(string query, T obj) where T : class
        {
            Type type = obj.GetType();
            Cluster cluster = CreateCluster(_settings.Cloud);
            ISession session = cluster.Connect(_settings.KeySpace);
            RowSet rowSet = new RowSet();

            await Task.Run(async () =>
            {
                var preparedStatement = session.Prepare(query);

                List<object> parameters = new List<object>();

                foreach (var propertyInfo in type.GetProperties())
                {
                    if (query.Contains($":{propertyInfo.Name}"))
                    {
                        parameters.Add(propertyInfo.GetValue(obj));
                    }
                }

                var statement = preparedStatement.Bind(parameters.ToArray());

                rowSet = await session.ExecuteAsync(statement);
            });            

            return rowSet;
        }

        private async Task<RowSet> ExecuteStatement<T>(string query, T[] list, string operation = "")
        {
            Type type = list.FirstOrDefault().GetType();
            Cluster cluster = CreateCluster(_settings.Cloud);
            ISession session = cluster.Connect(_settings.KeySpace);

            RowSet result = new RowSet();

            int remainingRows = list.Length - 1;
            int start = 0;
            int end = 0;
            int offset = 0;
            while (remainingRows > 0)
            {
                offset = remainingRows > DatabaseService.Offset ? DatabaseService.Offset : remainingRows;
                end = start + offset;

                await Task.Run(async () => 
                {
                    var batchStatement = new BatchStatement();
                    batchStatement.SetBatchType(BatchType.Unlogged);

                    var preparedStatement = session.Prepare(query);

                    for (int i = start; i <= end; i++)
                    {
                        List<object> parameters = new List<object>();

                        foreach (var propertyInfo in type.GetProperties())
                        {
                            if (query.Contains($":{propertyInfo.Name}"))
                            {
                                parameters.Add(propertyInfo.GetValue(list[i]));
                            }
                        }

                        var boundStatement = preparedStatement.Bind(parameters.ToArray());
                        batchStatement.Add(boundStatement);
                    }

                    result = await session.ExecuteAsync(batchStatement);
                });                

                start = end + 1;
                remainingRows = (list.Length - 1) - end;
            }

            return result;
        }

        public void Join<TP, T>(ref TP parent, T child) where T : class
        {
            Type parentType = parent.GetType();
            Type childType = child.GetType();

            List<T> result = new List<T>();

            var childRecords = Get(child).Result;

            var parentId = Guid.Parse(parentType.GetProperties().Where(p => p.Name == "Id").FirstOrDefault().GetValue(parent).ToString());

            foreach (var childRecord in childRecords)
            {
                foreach (var childRecordProperty in childType.GetProperties())
                {
                    if (childRecordProperty.Name == $"{parentType.Name}Id")
                    {
                        if (Guid.Parse(childRecordProperty.GetValue(childRecord).ToString()) == parentId)
                        {
                            result.Add(childRecord);
                            break;
                        }
                    }
                }
            }

            if (result.Count == 1)
            {
                var childRecordProperty = parentType.GetProperties().Where(p => p.PropertyType == childType).FirstOrDefault();
                childRecordProperty.SetValue(parent, result.FirstOrDefault());
            }
            else if (result.Count > 1)
            {
                var childRecordProperty = parentType.GetProperties().Where(p => p.PropertyType == childType).FirstOrDefault();
                childRecordProperty.SetValue(parent, result.AsEnumerable());
            }
        }

        public void Join<TP, T>(ref IEnumerable<TP> parentList, T child) where T : class
        {
            Type parentType = parentList.FirstOrDefault().GetType();
            Type childType = child.GetType();

            var childRecords = Get(child).Result;

            foreach (var parentRecord in parentList)
            {
                List<T> result = new List<T>();

                var parentId = Guid.Parse(parentType.GetProperties().Where(p => p.Name == "Id").FirstOrDefault().GetValue(parentRecord).ToString());

                foreach (var childRecord in childRecords)
                {
                    foreach (var childRecordProperty in childType.GetProperties())
                    {
                        if (childRecordProperty.Name == $"{parentType.Name}Id")
                        {
                            if (Guid.Parse(childRecordProperty.GetValue(childRecord).ToString()) == parentId)
                            {
                                result.Add(childRecord);
                                break;
                            }
                        }
                    }
                }

                if (result.Count == 1)
                {
                    var childRecordProperty = parentType.GetProperties().Where(p => p.PropertyType == childType).FirstOrDefault();
                    childRecordProperty.SetValue(parentRecord, result.FirstOrDefault());
                }
                else if (result.Count > 1)
                {
                    var childRecordProperty = parentType.GetProperties().Where(p => p.PropertyType == childType).FirstOrDefault();
                    childRecordProperty.SetValue(parentRecord, result.FirstOrDefault());
                }
            }
        }

        private Cluster CreateCluster(string cloud)
        {
            switch (cloud)
            {
                case "Azure":
                    var options = new Cassandra.SSLOptions(System.Security.Authentication.SslProtocols.Tls12, true, null);
                    options.SetHostNameResolver((ipAddress) => _settings.ContactPoint);
                    return Cluster.Builder()
                        .WithCredentials(_settings.Username, _settings.Password)
                        .WithPort(_settings.Port)
                        .AddContactPoints(_settings.ContactPoint)
                        .WithSSL(options)
                        .Build();
                case "AWS":
                    return Cluster.Builder()
                        .WithCredentials(_settings.Username, _settings.Password)
                        .WithPort(_settings.Port)
                        .AddContactPoints(_settings.ContactPoint)
                        .Build();
                case "Google":
                    return Cluster.Builder()
                        .WithCredentials(_settings.Username, _settings.Password)
                        .WithPort(_settings.Port)
                        .AddContactPoints(_settings.ContactPoint)
                        .Build();
                default:
                    return Cluster.Builder()
                        .AddContactPoints(_settings.ContactPoint)
                        .WithPort(_settings.Port)
                        .Build();
                    
            }

            return null;
        }
    }
}
