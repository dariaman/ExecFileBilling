using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExecFileBilling
{
    class Program
    {
        static string constring = ConfigurationManager.AppSettings["DefaultDB"];
        static void Main(string[] args)
        {
        }
        public static void genFile()
        {
            MySqlConnection con = new MySqlConnection(constring);
        }
    }
}
