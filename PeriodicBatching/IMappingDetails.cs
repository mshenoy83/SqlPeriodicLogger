using System.Collections.Generic;

namespace PeriodicDatabaseLogger
{
    public interface IMappingDetails
    {
        List<string> ColumnMappings();

        string TableName();
    }
}
