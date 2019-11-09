using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PeriodicDatabaseLogger
{
    public class TempLog : IMappingDetails
    {
        public string Url { get; set; }
        public string Response { get; set; }
        public int TimeTaken { get; set; }

        public List<string> ColumnMappings()
        {
            return new List<string>
            {
                nameof(Response),
                nameof(TimeTaken),
                nameof(Url)
            };
        }

        public string TableName()
        {
            return "TempLog";
        } 
    }
}
