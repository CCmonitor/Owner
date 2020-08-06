using System.Configuration;
namespace DBHelper
{
    public class DbHelper
    {
        /// <summary>
        /// 从配置文件中读取数据库连接字符串
        /// </summary>
        public static string ConnectionString
        {
            get { return ConfigurationManager.ConnectionStrings["DataBase"].ConnectionString; }
        }
    }
}
