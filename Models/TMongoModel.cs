using System;

namespace CloudPortAPI.Models
{
    public class TMongoModel
    {
        //[BsonId]
        //[BsonRepresentation(BsonType.ObjectId)]
        public Guid Id { get; set; }
    }
}
