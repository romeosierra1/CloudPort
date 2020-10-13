using CloudPortAPI.Config;
using CloudPortAPI.Models;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    class SqlDatabaseService //: IDatabaseService
    {
        private SqlDatabaseSettings _settings;
        public SqlDatabaseService(SqlDatabaseSettings settings)
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
                await RunCommand(query, obj);
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
                await RunCommand(query, list, "insert");
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
                    if (_settings.SqlEngine.ToLower() == "sqlserver")
                    {
                        query += $"@{propertyInfo.Name}{postFix},";
                    }
                    else if (_settings.SqlEngine.ToLower() == "mysql")
                    {
                        query += $"@{propertyInfo.Name}{postFix},";
                    }
                }
            }
            query = query.TrimEnd(',');
            query += ");";

            return query;
        }

        public async Task<IEnumerable<T>> Get<T>(T obj) where T : class
        {
            Type type = obj.GetType();

            List<T> result = new List<T>();

            if (type.IsClass)
            {
                string query = $"SELECT * FROM {type.Name}";

                DataTable dt = await RunCommand(query, obj);

                await Task.Run(() => 
                {
                    foreach (DataRow dataRow in dt.Rows)
                    {
                        var item = Activator.CreateInstance<T>();

                        foreach (DataColumn column in dataRow.Table.Columns)
                        {
                            PropertyInfo property = item.GetType().GetProperties().FirstOrDefault(p => p.Name == column.ColumnName);

                            if (property != null && dataRow[column] != DBNull.Value && dataRow[column].ToString() != "NULL")
                            {
                                Guid temp = Guid.Empty;
                                if (Guid.TryParse(dataRow[column].ToString(), out temp))
                                {
                                    (item as TSqlModel).Id = temp;
                                }
                                else
                                {
                                    property.SetValue(item, dataRow[column]);
                                }
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

                await RunCommand(query, obj);
            }

            return result;
        }

        public async Task<int> Remove<T>(T[] list) where T : class
        {
            int result = 0;

            if (_settings.SqlEngine.ToLower() == "sqlserver")
            {
                Type type = list.FirstOrDefault().GetType();

                if (type.IsClass)
                {
                    string query = GenerateDeleteQuery(type);

                    //RunCommand(query, list);                    
                    await Task.Run(() =>
                    {
                        using (SqlConnection conn = new SqlConnection(_settings.ConnectionString))
                        {
                            SqlDataAdapter adp = new SqlDataAdapter($"SELECT * FROM {type.Name}", conn);
                            SqlCommandBuilder cmdBuilder = new SqlCommandBuilder(adp);
                            DataTable dt = new DataTable();
                            SqlCommand cmd = new SqlCommand(query, conn);
                            cmd.Parameters.Add(new SqlParameter("@Id", SqlDbType.UniqueIdentifier));
                            cmd.Parameters["@Id"].SourceVersion = DataRowVersion.Original;
                            cmd.Parameters["@Id"].SourceColumn = "Id";
                            adp.DeleteCommand = cmd;
                            adp.Fill(dt);
                                //Console.WriteLine(cmdBuilder.GetDeleteCommand().CommandText);
                                foreach (DataRow row in dt.Rows)
                            {
                                row.Delete();
                            }
                            adp.Update(dt);
                        }
                    });
                }
            }
            else if (_settings.SqlEngine.ToLower() == "mysql")
            {
                Type type = list.FirstOrDefault().GetType();

                if (type.IsClass)
                {
                    string query = GenerateDeleteQuery(type);

                    //RunCommand(query, list);

                    await Task.Run(() => 
                    {
                        using (MySqlConnection conn = new MySqlConnection(_settings.ConnectionString))
                        {
                            MySqlDataAdapter adp = new MySqlDataAdapter($"SELECT * FROM {type.Name}", conn);
                            DataTable dt = new DataTable();
                            MySqlCommand cmd = new MySqlCommand(query, conn);
                            cmd.Parameters.Add(new MySqlParameter("@Id", MySqlDbType.Guid));
                            cmd.Parameters["@Id"].SourceVersion = DataRowVersion.Original;
                            cmd.Parameters["@Id"].SourceColumn = "Id";
                            adp.DeleteCommand = cmd;
                            adp.Fill(dt);
                            foreach (DataRow row in dt.Rows)
                            {
                                row.Delete();
                            }
                            adp.Update(dt);
                        }
                    });                    
                }
            }

            return result;
        }

        private string GenerateDeleteQuery(Type type, string postFix = "")
        {
            if (_settings.SqlEngine.ToLower() == "sqlserver")
            {
                return $"DELETE FROM {type.Name} WHERE Id = @Id";
            }
            else if (_settings.SqlEngine.ToLower() == "mysql")
            {
                return $"DELETE FROM {type.Name} WHERE Id = @Id";
            }
            return null;
        }

        public async Task<int> Update<T>(T obj) where T : class
        {
            int result = 0;

            Type type = obj.GetType();

            if (type.IsClass)
            {
                string query = GenerateUpdateQuery(type);
                await RunCommand(query, obj);
            }

            return result;
        }

        public async Task<int> Update<T>(T[] list) where T : class
        {
            int result = 0;

            Type type = list.FirstOrDefault().GetType();

            if (type.IsClass)
            {
                string query = GenerateUpdateQuery(type);
                await RunCommand(query, list, "update");
            }

            return result;
        }

        private string GenerateUpdateQuery(Type type, string postFix = "")
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

                if (IsParameter)
                {
                    if (_settings.SqlEngine.ToLower() == "sqlserver")
                    {
                        query += propertyInfo.Name + " = @" + propertyInfo.Name + ",";
                    }
                    else if (_settings.SqlEngine.ToLower() == "mysql")
                    {
                        query += propertyInfo.Name + " = @" + propertyInfo.Name + ",";
                    }

                }
            }
            query = query.TrimEnd(',');
            query += " WHERE ";
            if (_settings.SqlEngine.ToLower() == "sqlserver")
            {
                query += "Id = @Id";
            }
            else if (_settings.SqlEngine.ToLower() == "mysql")
            {
                query += "Id = @Id";
            }


            return query;
        }

        private async Task<DataTable> RunCommand<T>(string query, T obj) where T : class
        {
            if (_settings.SqlEngine.ToLower() == "sqlserver")
            {
                DataTable dt = new DataTable();

                Type type = obj.GetType();

                SqlConnection sqlConnection = new SqlConnection(_settings.ConnectionString);
                SqlCommand sqlCommand = new SqlCommand(query, sqlConnection);

                await Task.Run(() => 
                {
                    foreach (var propertyInfo in type.GetProperties())
                    {
                        if (query.Contains($"@{propertyInfo.Name}"))
                        {
                            sqlCommand.Parameters.Add(new SqlParameter($"@{propertyInfo.Name}", propertyInfo.GetValue(obj).ToString()));
                        }
                    }

                    SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(sqlCommand);
                    sqlDataAdapter.Fill(dt);
                });                

                return dt;
            }
            else if (_settings.SqlEngine.ToLower() == "mysql")
            {
                DataTable dt = new DataTable();
                Type type = obj.GetType();
                MySqlConnection mySqlConnection = new MySqlConnection(_settings.ConnectionString);
                MySqlCommand mySqlCommand = new MySqlCommand(query, mySqlConnection);
                //mySqlConnection.Open();
                //mySqlCommand.Prepare();

                await Task.Run(() => 
                {
                    foreach (var propertyInfo in type.GetProperties())
                    {
                        if (query.Contains($"@{propertyInfo.Name}"))
                        {
                            mySqlCommand.Parameters.Add(new MySqlParameter($"@{propertyInfo.Name}", propertyInfo.GetValue(obj).ToString()));
                        }
                    }
                    MySqlDataAdapter mySqlDataAdapter = new MySqlDataAdapter(mySqlCommand);
                    mySqlDataAdapter.Fill(dt);
                });
                
                //mySqlConnection.Close();
                return dt;
            }
            return null;
        }

        public DataTable CustomSqlQuery(string query)
        {
            DataTable dt = new DataTable();

            SqlConnection sqlConnection = new SqlConnection(_settings.ConnectionString);
            SqlCommand sqlCommand = new SqlCommand(query, sqlConnection);

            SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(sqlCommand);
            sqlDataAdapter.Fill(dt);

            return dt;
        }

        public DataTable GetSchema(string collection)
        {
            DataTable dt = new DataTable(); ;
            SqlConnection sqlConnection = new SqlConnection(_settings.ConnectionString);
            sqlConnection.Open();
            dt = sqlConnection.GetSchema(collection);
            sqlConnection.Close();
            return dt;
        }

        private async Task<DataTable> RunCommand<T>(string query, T[] list, string operation = "") where T : class
        {
            if (_settings.SqlEngine.ToLower() == "sqlserver")
            {
                DataTable dt = new DataTable();

                Type type = list.FirstOrDefault().GetType();

                SqlConnection sqlConnection = new SqlConnection(_settings.ConnectionString);
                SqlCommand sqlCommand = new SqlCommand(query, sqlConnection);

                SqlDataAdapter sqlDataAdapter = new SqlDataAdapter($"SELECT * FROM {type.Name}", sqlConnection);

                //if (operation == "insert")
                //{
                sqlDataAdapter.InsertCommand = sqlCommand;
                //}
                //else if (operation == "update")
                //{
                sqlDataAdapter.UpdateCommand = sqlCommand;
                //}

                await Task.Run(() =>
                {
                    for (int i = 0; i < type.GetProperties().Length; i++)
                    {
                        var propertyInfo = type.GetProperties()[i];
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
                            var sqlParam = new SqlParameter();
                            var sqlParam1 = new SqlParameter();
                            sqlParam.ParameterName = "@" + propertyInfo.Name;
                            sqlParam1.ParameterName = "@" + propertyInfo.Name + "1";
                            sqlParam.SourceColumn = propertyInfo.Name;
                            sqlParam1.SourceColumn = propertyInfo.Name;

                            sqlDataAdapter.InsertCommand.Parameters.Add(sqlParam);

                            if (sqlParam1.ParameterName == "@Id")
                            {
                                sqlParam1.SourceVersion = DataRowVersion.Original;
                            }
                            sqlDataAdapter.UpdateCommand.Parameters.Add(sqlParam1);
                        }
                    }

                    for (int i = 0; i < type.GetProperties().Length; i++)
                    {
                        var propertyInfo = type.GetProperties()[i];
                        dt.Columns.Add(propertyInfo.Name, propertyInfo.PropertyType);
                    }

                    int remainingRows = list.Length - 1;
                    int start = 0;
                    int end = 0;
                    int offset = 0;
                    while (remainingRows > 0)
                    {
                        offset = remainingRows > DatabaseService.Offset ? DatabaseService.Offset : remainingRows;
                        end = start + offset;
                        for (int i = start; i <= end; i++)
                        {
                            var obj = list[i];
                            var dr = dt.NewRow();
                            foreach (var propertyInfo in type.GetProperties())
                            {
                                dr[propertyInfo.Name] = propertyInfo.GetValue(obj);
                            }
                            dt.Rows.Add(dr);
                        }
                        sqlDataAdapter.Update(dt);
                        start = end + 1;
                        remainingRows = (list.Length - 1) - end;
                    }
                });

                return dt;
            }
            else if (_settings.SqlEngine.ToLower() == "mysql")
            {
                DataTable dt = new DataTable();

                Type type = list.FirstOrDefault().GetType();

                MySqlConnection mySqlConnection = new MySqlConnection(_settings.ConnectionString);
                MySqlCommand mySqlCommand = new MySqlCommand(query, mySqlConnection);

                MySqlDataAdapter mySqlDataAdapter = new MySqlDataAdapter($"SELECT * FROM {type.Name}", mySqlConnection);

                //if (operation == "insert")
                //{
                mySqlDataAdapter.InsertCommand = mySqlCommand;
                //}
                //else if (operation == "update")
                //{
                mySqlDataAdapter.UpdateCommand = mySqlCommand;
                //}

                await Task.Run(() =>
                {
                    for (int i = 0; i < type.GetProperties().Length; i++)
                    {
                        var propertyInfo = type.GetProperties()[i];
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
                            var sqlParam = new MySqlParameter();
                            var sqlParam1 = new MySqlParameter();
                            sqlParam.ParameterName = "@" + propertyInfo.Name;
                            sqlParam1.ParameterName = "@" + propertyInfo.Name + "1";
                            sqlParam.SourceColumn = propertyInfo.Name;
                            sqlParam1.SourceColumn = propertyInfo.Name;

                            mySqlDataAdapter.InsertCommand.Parameters.Add(sqlParam);

                            if (sqlParam1.ParameterName == "@Id")
                            {
                                sqlParam1.SourceVersion = DataRowVersion.Original;
                            }
                            mySqlDataAdapter.UpdateCommand.Parameters.Add(sqlParam1);
                        }
                    }

                    for (int i = 0; i < type.GetProperties().Length; i++)
                    {
                        var propertyInfo = type.GetProperties()[i];
                        dt.Columns.Add(propertyInfo.Name, propertyInfo.PropertyType);
                    }

                    int remainingRows = list.Length - 1;
                    int start = 0;
                    int end = 0;
                    int offset = 0;
                    while (remainingRows > 0)
                    {
                        offset = remainingRows > DatabaseService.Offset ? DatabaseService.Offset : remainingRows;
                        end = start + offset;
                        for (int i = start; i <= end; i++)
                        {
                            var obj = list[i];
                            var dr = dt.NewRow();
                            foreach (var propertyInfo in type.GetProperties())
                            {
                                dr[propertyInfo.Name] = propertyInfo.GetValue(obj);
                            }
                            dt.Rows.Add(dr);
                        }
                        mySqlDataAdapter.Update(dt);
                        start = end + 1;
                        remainingRows = (list.Length - 1) - end;
                    }
                });                

                return dt;
            }
            return null;
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
                    childRecordProperty.SetValue(parentRecord, result.AsEnumerable());
                }
            }
        }
    }
}
