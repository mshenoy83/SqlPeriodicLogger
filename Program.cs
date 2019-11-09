using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using PeriodicDatabaseLogger.PeriodicBatching;

namespace PeriodicDatabaseLogger
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var sqllog = new SqlServerPeriodicLogger<TempLog>(TimeSpan.FromSeconds(5), @"Data Source=DESKTOP-V8AL0SS\DEVSERVER;Initial Catalog=SampleDb;Integrated Security=True"))
            {
                sqllog.AddLog(new TempLog
                {
                    Response = "Hello World",
                    Url = "https://www.google.com.au",
                    TimeTaken = 5
                });
                
                sqllog.AddLog(new TempLog
                {
                    Response = "Hello World",
                    Url = "https://www.google.com.au",
                    TimeTaken = 5
                });

                sqllog.AddLog(new TempLog
                {
                    Response = "Hello World",
                    Url = "https://www.google.com.au",
                    TimeTaken = 5
                });

                sqllog.AddLog(new TempLog
                {
                    Response = "Hello World",
                    Url = "https://www.google.com.au",
                    TimeTaken = 5
                });
                sqllog.AddLog(new TempLog
                {
                    Response = "Hello World",
                    Url = "https://www.google.com.au",
                    TimeTaken = 5
                });
                sqllog.AddLog(new TempLog
                {
                    Response = "Hello World",
                    Url = "https://www.google.com.au",
                    TimeTaken = 5
                });

            }
        }
    }
}
