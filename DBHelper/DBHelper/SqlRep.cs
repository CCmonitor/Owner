using Dapper;
using log4net;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace DBHelper
{
    public class SqlRep
    {
        public static IEnumerable<string> GetTableNames(IDbConnection db)
        {
            string sql = @"SELECT Name FROM SysObjects Where XType='U' and name like 'UT_%' and name not like '%History' ORDER BY Name";
            var tableNames = db.Query<string>(sql);
            return tableNames;
        }
        public static IEnumerable<string> GetColumnNames(IDbConnection db)
        {
            string sql = @"SELECT COLUMN_NAME,TABLE_NAME FROM INFORMATION_SCHEMA.columns ";
            var tableNames = db.Query<string>(sql);
            return tableNames;
        }
        //public static IEnumerable<string> GetColumnNames(string tableNames)
        //{
        //    string sql = @"SELECT COLUMN_NAME,TABLE_NAME FROM INFORMATION_SCHEMA.columns ";
        //    var columns = db.Query<string>(sql);
        //    return columns;
        //}
        #region Aop And 历史记录表
        /// <summary>
        /// 判断Aop表存不存在,没有则创建
        /// </summary>
        /// <param name="db"></param>
        public static void JudgeAop(IDbConnection db)
        {
            string judgeAopSql = @"SELECT isnull(count(*),0) FROM SysObjects Where XType='U' and name='Aop'";
            var aopNum = db.QuerySingle<int>(judgeAopSql);
            if (aopNum < 1)
            {
                var sqlAop = @"CREATE TABLE [dbo].[Aop](
                                        	[Aop] [nvarchar](6) NULL,
                                        	[IsHand] [bit] NULL,
                                        	[HandPC] [nvarchar](200) NULL,
                                        	[OperateTime] [datetime] NULL
                                        )";
                try
                {
                    db.Execute(sqlAop);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("创建历史Aop失败:" + ex.Message);
                    throw ex;
                }
            }
        }


        public static void DeleteHistory(IDbConnection db, IEnumerable<string> tableNames)
        {
            foreach (var tableName in tableNames)
            {
                var historySql = @" drop table {0}History";
                historySql = string.Format(historySql, tableName);
                try
                {
                    db.Execute(historySql);
                    Console.WriteLine("删除成功: TABLE : " + tableName);
                }
                catch (Exception)
                {
                    //Console.WriteLine("删除历史记录表失败:" + ex.Message + " TABLE : " + tableName);
                }
            }
        }

        /// <summary>
        /// 根据实体表创建历史记录表
        /// </summary>
        /// <param name="db"></param>
        /// <param name="tableNames"></param> 
        public static void CreateHistory(IDbConnection db, IEnumerable<string> tableNames)
        {
            foreach (var tableName in tableNames)
            {
                var historySql = @" select * into {0}History  from  {0}
                          left outer join Aop on Aop.Aop={0}.f_id where 1=2";
                historySql = string.Format(historySql, tableName);
                try
                {
                    db.Execute(historySql);
                }
                catch (Exception ex)
                {
                    if (ex.Message.Contains("数据库中已存在名为"))
                    {
                        Console.WriteLine("创建历史记录表失败:" + ex.Message + " TABLE : " + tableName);
                        continue;
                    }
                    Console.WriteLine("创建历史记录表失败:" + ex.Message + " TABLE : " + tableName);
                }
            }
        }
        #endregion
        public void DeleteTrigger(IDbConnection db, IEnumerable<string> tableNames)
        {
            foreach (var tableName in tableNames)
            {
                try
                {
                    var deleteTrigger = @"drop trigger {0}DeleteTrigger";
                    deleteTrigger = string.Format(deleteTrigger, tableName);
                    db.Execute(deleteTrigger);

                    var insertTrigger = @"drop trigger {0}InsertTrigger";
                    insertTrigger = string.Format(insertTrigger, tableName);
                    db.Execute(insertTrigger);

                    var updateTrigger = @"drop trigger {0}UpdateTrigger";
                    updateTrigger = string.Format(updateTrigger, tableName);
                    db.Execute(updateTrigger);
                    Console.WriteLine("删除触发器成功: TABLE : " + tableName);
                }
                catch (Exception)
                {
                }
            }
        }

        /// <summary>
        /// 创建insert触发器
        /// </summary>
        /// <param name="db"></param>
        /// <param name="tableNames"></param>
        public static void CreateInsertTrigger(IDbConnection db, IEnumerable<string> tableNames)
        {
            foreach (var tableName in tableNames)
            {
                //--delete触发器
                var insertTrigger = @" create or ALTER TRIGGER [dbo].[{0}InsertTrigger]
                                                 ON [dbo].[{0}]   after  INSERT
                                                 AS
                                                 BEGIN
                                                 DECLARE @ishand INT
                                                 SELECT @ishand=COUNT(*) FROM Master..SysProcesses WHERE Spid = @@spid AND program_name LIKE '.Net%'
                                                 IF(@ishand>0)
                                                 BEGIN
                                                 INSERT INTO [dbo].[{0}History]
                                                 SELECT *,
                                                 'Add' AS [Aop],0 AS [IsHand],'' AS [HandPC],GETDATE() as OperateTime
                                                 FROM inserted
                                                 END
                                                 ELSE
                                                 BEGIN
                                                 DECLARE @hostname nvarchar(200)
                                                 SELECT @hostname=hostname FROM Master..SysProcesses WHERE Spid = @@spid
                                                  INSERT INTO [dbo].[{0}History]
                                                 SELECT *,
                                                 'Add' AS Aop,1 AS [IsHand],@hostname AS [HandPC],GETDATE() as OperateTime
                                                 FROM inserted
                                                  END
                                                 END";
                insertTrigger = string.Format(insertTrigger, tableName);
                try
                {
                    db.Execute(insertTrigger, new { TableName = tableName });
                    Console.WriteLine(" TABLE : " + tableName + "   Insert触发器创建成功");
                }
                catch (Exception ex)
                {
                    if (ex.Message.Contains("数据库中已存在名为"))
                    {
                        Console.WriteLine(ex.Message + " TABLE : " + tableName);
                        continue;
                    }
                    Console.WriteLine(ex.Message + " TABLE : " + tableName);
                }

            }
        }
        /// <summary>
        /// 创建Delete触发器
        /// </summary>
        /// <param name="db"></param>
        /// <param name="tableNames"></param>
        public static void CreateDeleteTrigger(IDbConnection db, IEnumerable<string> tableNames)
        {
            foreach (var tableName in tableNames)
            {
                //--delete触发器
                var deleteTrigger = @" CREATE OR ALTER TRIGGER [dbo].[{0}DeleteTrigger]
                                                    ON [dbo].[{0}]   after  DELETE
                                                    AS
                                                    BEGIN
                                                    DECLARE @ishand INT
                                                    SELECT @ishand=COUNT(*) FROM Master..SysProcesses WHERE Spid = @@spid AND program_name LIKE '.Net%'
                                                    IF(@ishand>0)
                                                    BEGIN
                                                    INSERT INTO [dbo].[{0}History]
                                                    SELECT *,
                                                    'Delete' AS [Aop],0 AS [IsHand],'' AS [HandPC],GETDATE() as OperateTime
                                                    FROM deleted
                                                    END
                                                    ELSE
                                                    BEGIN
                                                    DECLARE @hostname nvarchar(200)
                                                    SELECT @hostname=hostname FROM Master..SysProcesses WHERE Spid = @@spid
                                                    INSERT INTO [dbo].[{0}History]
                                                    SELECT *,
                                                    'Delete' AS Aop,1 AS [IsHand],@hostname AS [HandPC],GETDATE() as OperateTime
                                                    FROM deleted
                                                    END
                                                    END";
                deleteTrigger = string.Format(deleteTrigger, tableName);
                try
                {
                    db.Execute(deleteTrigger, new { TableName = tableName });
                    Console.WriteLine(" TABLE : " + tableName + "   Delete触发器创建成功");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message + " TABLE : " + tableName);
                }

            }
        }
        /// <summary>
        /// 创建Update触发器
        /// </summary>
        /// <param name="db"></param>
        /// <param name="tableNames"></param>
        public static void CreateUpdateTrigger(IDbConnection db, IEnumerable<string> tableNames)
        {
            foreach (var tableName in tableNames)
            {

                var updateTrigger = @" Create or ALTER TRIGGER [dbo].[{0}UpdateTrigger]
                                                    ON [dbo].[{0}]   after  Update
                                                    AS
                                                    BEGIN
                                                   DECLARE @ishand INT 
                                                   SELECT @ishand = COUNT (*)
                                                   FROM
                                                   	Master..SysProcesses
                                                   WHERE
                                                   	Spid = @@spid
                                                   AND program_name LIKE '.Net%'
                                                   IF (@ishand > 0)
                                                   BEGIN
                                                   	INSERT INTO [dbo].[{0}History]
                                                   SELECT *,
                                                   		'Update' AS Aop,
                                                   		0 AS [IsHand],
                                                   		'' AS [HandPC],GETDATE() as OperateTime
                                                   	FROM
                                                   		inserted 
                                                   END
                                                   ELSE
                                                   
                                                   BEGIN
                                                   DECLARE @updatetime datetime = GETDATE()
                                                   DECLARE @hostname nvarchar (200) SELECT
                                                   	@hostname = hostname
                                                   FROM
                                                   	Master..SysProcesses
                                                   WHERE
                                                   	Spid = @@spid 
                                                   INSERT INTO [dbo].[{0}History]
                                                   SELECT *,
                                                   		'Update' AS Aop,
                                                   		1 AS [IsHand],
                                                   		@hostname AS [HandPC],GETDATE() as OperateTime
                                                   	FROM
                                                   		inserted 
                                                   end
                                                   END";
                updateTrigger = string.Format(updateTrigger, tableName);
                try
                {
                    db.Execute(updateTrigger, new { TableName = tableName });
                    Console.WriteLine(" TABLE : " + tableName + "   Update触发器创建成功");
                }
                catch (Exception ex)
                {
                    if (ex.Message.Contains("数据库中已存在名为"))
                    {
                        continue;
                    }
                    Console.WriteLine(ex.Message + " TABLE : " + tableName);
                }

            }
        }

        /// <summary>
        /// 判断历史记录表和主表结构是否一致
        /// </summary>
        /// <param name="db"></param>
        /// <param name="tableNames"></param>
        public static void ExceptTable(IDbConnection db, IEnumerable<string> tableNames)
        {
            foreach (var tableName in tableNames)
            {
                #region exceptSql
                var exceptSql = @" ( SELECT column_name, column_type, max_length, is_nullable, 'Master' AS tabletype
                                            	FROM
                                            		( SELECT name AS column_name, TYPE_NAME(system_type_id) AS column_type, max_length, is_nullable
                                            			FROM
                                            				sys.columns
                                            			WHERE
                                            				object_id = OBJECT_ID(N'{0}')
                                            			EXCEPT
                                            				SELECT name AS column_name, TYPE_NAME(system_type_id) AS column_type, max_length, is_nullable
                                            				FROM
                                            					sys.columns
                                            				WHERE
                                            					object_id = OBJECT_ID( N'{0}History' ) ) t )
                                            UNION
                                            	( SELECT column_name, column_type, max_length, is_nullable, 'History' AS tabletype
                                            		FROM
                                            			( SELECT name AS column_name, TYPE_NAME(system_type_id) AS column_type, max_length, is_nullable
                                            				FROM
                                            					sys.columns
                                            				WHERE
                                            					object_id = OBJECT_ID( N'{0}History' )
                                            				EXCEPT
                                            					SELECT name AS column_name, TYPE_NAME(system_type_id) AS column_type, max_length, is_nullable
                                            					FROM
                                            						sys.columns
                                            					WHERE
                                            						object_id = OBJECT_ID(N'{0}') ) t )";
                #endregion
                exceptSql = string.Format(exceptSql, tableName);
                try
                {
                    var tableExcepts = db.Query<TableExcept>(exceptSql).ToList();
                    if (tableExcepts.Count() > 4)
                    {

                        ILog log = LogManager.GetLogger("test");
                        log.Info(tableName + "主表和历史记录表结构不匹配");
                        Console.WriteLine(tableName + "主表和历史记录表结构不匹配");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("主表和历史记录表比对结果失败:" + ex.Message + " TABLE : " + tableName);
                }
            }
        }

        public static void ExceptTable(IDbConnection db, string tableName)
        {
            #region exceptSql
            var exceptSql = @" ( SELECT column_name, column_type, max_length, is_nullable, 'Master' AS tabletype
                                            	FROM
                                            		( SELECT name AS column_name, TYPE_NAME(system_type_id) AS column_type, max_length, is_nullable
                                            			FROM
                                            				sys.columns
                                            			WHERE
                                            				object_id = OBJECT_ID(N'UT_FI_PaymentApply')
                                            			EXCEPT
                                            				SELECT name AS column_name, TYPE_NAME(system_type_id) AS column_type, max_length, is_nullable
                                            				FROM
                                            					sys.columns
                                            				WHERE
                                            					object_id = OBJECT_ID( N'UT_FI_PaymentApplyHistory' ) ) t )
                                            UNION
                                            	( SELECT column_name, column_type, max_length, is_nullable, 'History' AS tabletype
                                            		FROM
                                            			( SELECT name AS column_name, TYPE_NAME(system_type_id) AS column_type, max_length, is_nullable
                                            				FROM
                                            					sys.columns
                                            				WHERE
                                            					object_id = OBJECT_ID( N'UT_FI_PaymentApplyHistory' )
                                            				EXCEPT
                                            					SELECT name AS column_name, TYPE_NAME(system_type_id) AS column_type, max_length, is_nullable
                                            					FROM
                                            						sys.columns
                                            					WHERE
                                            						object_id = OBJECT_ID(N'UT_FI_PaymentApply') ) t )";
            #endregion
            exceptSql = string.Format(exceptSql, tableName);
            try
            {
                var tableExcepts = db.Query<TableExcept>(exceptSql).ToList();
                if (tableExcepts.Count() > 4)
                {
                    Console.WriteLine(tableName + "主表和历史记录表结构不匹配");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("主表和历史记录表比对结果失败:" + ex.Message + " TABLE : " + tableName);
            }
        }
    }
}
