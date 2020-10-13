using MongoDB.Driver;
using System;
using System.Collections.Generic;
using CloudPortAPI.Models;
using System.Linq;
using System.Reflection;
using MongoDB.Bson;
using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    class DocumentDatabaseService //: IDatabaseService
    {
        private readonly IMongoDatabase _database;

        public DocumentDatabaseService(Config.MongoDatabaseSettings mongoSettings)
        {
            MongoClient client = null;
            MongoClientSettings settings = MongoClientSettings.FromUrl(new MongoUrl(mongoSettings.ConnectionString));

            switch (mongoSettings.Cloud)
            {
                case "Azure":
                    settings.SslSettings = new SslSettings() { EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12 };
                    client = new MongoClient(settings);
                    break;
                case "AWS":
                    //settings.AllowInsecureTls = true;
                    client = new MongoClient(settings);
                    break;
                case "Google":
                    client = new MongoClient(mongoSettings.ConnectionString);
                    break;
                default:
                    client = new MongoClient(mongoSettings.ConnectionString);
                    break;
            }
            
            _database = client.GetDatabase(mongoSettings.DatabaseName);
        }

        private IMongoCollection<T> GetCollection<T>(T model) where T : class
        {
            Type type = model.GetType();

            if (!type.IsClass && typeof(TMongoModel) != type.BaseType)
            {
                throw new InvalidCastException("Model must be inherited from TMongoModel");
            }

            return _database.GetCollection<T>(model.GetType().Name);
        }

        public async Task<int> Add<T>(T obj) where T : class
        {
            var collection = GetCollection(obj);
            int result = 0;

            await collection.InsertOneAsync(obj);

            return result;
        }

        public async Task<int> Add<T>(T[] list) where T : class
        {
            var obj = list.FirstOrDefault();
            var collection = GetCollection(obj);
            int result = 0;

            int remainingRows = list.Length - 1;
            int start = 0;
            int end = 0;
            int offset = 0;
            while (remainingRows > 0)
            {
                offset = remainingRows > DatabaseService.Offset ? DatabaseService.Offset : remainingRows;
                end = start + offset;

                List<T> data = new List<T>();

                for (int i = start; i <= end; i++)
                {
                    data.Add(list[i]);
                }

                await collection.InsertManyAsync(data);

                start = end + 1;
                remainingRows = (list.Length - 1) - end;
            }

            return result;
        }

        public async Task<IEnumerable<T>> Get<T>(T obj) where T : class
        {
            var collection = GetCollection(obj);

            IEnumerable<T> list = (await collection.FindAsync(new BsonDocument())).ToList<T>();

            return list;
        }

        public async Task<int> Remove<T>(T obj) where T : class
        {
            var collection = GetCollection(obj);
            int result = 0;

            Type type = obj.GetType();

            PropertyInfo propertyInfo = type.GetProperties().FirstOrDefault(p => p.Name.Equals("Id"));

            //string id = propertyInfo.GetValue(obj).ToString();
            Guid id = Guid.Parse(propertyInfo.GetValue(obj).ToString());

            await collection.DeleteOneAsync(o => (o as TMongoModel).Id == id);

            return result;
        }

        public async Task<int> Remove<T>(T[] list) where T : class
        {
            var obj = list.FirstOrDefault();

            var collection = GetCollection(obj);
            int result = 0;

            var bulkDelete = new List<WriteModel<T>>();

            //foreach (var item in list)
            {
                //var removeOne = new DeleteOneModel<T>(Builders<T>.Filter.Where(x => (x as TMongoModel).Id == (item as TMongoModel).Id));
                var iDs = list.Select(o => (o as TMongoModel).Id).ToList();
                var filterDefinition = Builders<T>.Filter.In(p => (p as TMongoModel).Id, iDs);
                var deleteMany = new DeleteManyModel<T>(filterDefinition);
                bulkDelete.Add(deleteMany);
            }

            await collection.BulkWriteAsync(bulkDelete);

            return result;
        }

        public async Task<int> Update<T>(T obj) where T : class
        {
            var collection = GetCollection(obj);
            int result = 0;

            Type type = obj.GetType();

            PropertyInfo propertyInfo = type.GetProperties().FirstOrDefault(p => p.Name.Equals("Id"));

            //string id = propertyInfo.GetValue(obj).ToString();
            Guid id = Guid.Parse(propertyInfo.GetValue(obj).ToString());
            await collection.ReplaceOneAsync(o => (o as TMongoModel).Id == id, obj);

            return result;
        }

        public async Task<int> Update<T>(T[] list) where T : class
        {
            var obj = list.FirstOrDefault();

            var collection = GetCollection(obj);
            int result = 0;

            //var bulkUpdate = new List<WriteModel<T>>();

            //foreach (var item in list)
            //{
            //    var replaceOne = new ReplaceOneModel<T>(Builders<T>.Filter.Where(x => (x as TMongoModel).Id == (item as TMongoModel).Id), item) { IsUpsert = true };
            //    bulkUpdate.Add(replaceOne);
            //}

            //collection.BulkWrite(bulkUpdate);

            await Remove(list);
            await Add(list);

            return result;
        }

        public void Join<TP, T>(ref TP parent, T child) where T : class
        {
            Type parentType = parent.GetType();
            Type childType = child.GetType();

            List<T> result = new List<T>();

            var childCollection = GetCollection(child);
            var childRecords = childCollection.Find(new BsonDocument()).ToList<T>();

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

            var childCollection = GetCollection(child);
            var childRecords = childCollection.Find(new BsonDocument()).ToList<T>();

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
