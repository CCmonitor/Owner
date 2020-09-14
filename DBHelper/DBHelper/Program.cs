using System;
using System.Data;
using System.Data.SqlClient;

namespace DBHelper
{

    class Program
    {
        public SqlRep SqlRep = new SqlRep();
        static void Main(string[] args)
        {
            using (IDbConnection db = new SqlConnection(DbHelper.ConnectionString))
            {
                PrintScreen();
                var input = Console.ReadLine().Trim();
                var tableNames = SqlRep.GetTableNames(db);
                try
                {
                    while (string.IsNullOrWhiteSpace(input) == false)
                    {
                        switch (input)
                        {
                            case "1":
                                SqlRep.ExceptTable(db, tableNames);
                                break;
                            case "2":
                                SqlRep.JudgeAop(db);
                                SqlRep.CreateHistory(db, tableNames);
                                break;
                            case "3":
                                SqlRep.CreateInsertTrigger(db, tableNames);
                                break;
                            case "4":
                                SqlRep.CreateUpdateTrigger(db, tableNames);
                                break;
                            case "5":
                                SqlRep.CreateDeleteTrigger(db, tableNames);
                                break;
                            case "6":
                                SqlRep.DeleteHistory(db, tableNames);
                                break;
                            default:
                                break;
                        }

                        PrintScreen();
                        input = Console.ReadLine().Trim();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            Console.ReadKey();
        }

        private static void PrintScreen()
        {
            Console.WriteLine("--------------------------------------------------------------------");
            Console.WriteLine("请输入要处理的类型");
            Console.WriteLine("1.判断数据库实体表和主表结构是否一致"); //
            Console.WriteLine("2.创建历史记录表");//
            Console.WriteLine("3.创建Insert触发器");//
            Console.WriteLine("4.创建Update触发器");//
            Console.WriteLine("5.创建Delete触发器");//
            Console.WriteLine("6.删除历史记录表");//
        }
    }
}
