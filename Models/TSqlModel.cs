using System;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace CloudPortAPI.Models
{
    public class TSqlModel
    {
        [Key]
        public Guid Id { get; set; }
        //public DateTime UpdatedOn { get; set; }
        //public DateTime DeletedOn { get; set; }
    }
}
