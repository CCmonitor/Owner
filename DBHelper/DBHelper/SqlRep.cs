using Dapper;
using log4net;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

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
        public static IEnumerable<Columns> GetColumnNames(IDbConnection db)
        {
            string sql = @"SELECT COLUMN_NAME,TABLE_NAME,DATA_TYPE,IS_NULLABLE,CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.columns ";
            return db.Query<Columns>(sql);
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
            var hasText = false;
            var columnAll = GetColumnNames(db);
            foreach (var tableName in tableNames)
            {
                var sb1 = new StringBuilder();
                var columns = columnAll.Where(r => r.TABLE_NAME.ToUpper() == tableName.ToUpper());
                var historySql = @"CREATE TABLE [dbo].[{0}History](
                                            [F_Id] [nvarchar](50) NOT NULL,{1}
                                            [OperateTime] [DateTime] NOT NULL, 
                                            [Aop] [nvarchar](6) NOT NULL, 
                                            [IsHand] [bit] {3} NULL,
                                            [HandPC][nvarchar](200) {3} NULL ) 
                                            ON [PRIMARY] {2}";
                foreach (var column in columns)
                {
                    if (column.COLUMN_NAME != "F_Id" &&
                        column.COLUMN_NAME != "F_CreateTime" &&
                        column.COLUMN_NAME != "F_ModifyTime" &&
                        column.COLUMN_NAME != "F_CreateUserID" &&
                        column.COLUMN_NAME != "F_ModifyUserId")
                    {
                        var sqlType = "";
                        if (column.DATA_TYPE == "bit")
                        {
                            sqlType = "[bit]";
                        }
                        else if (column.DATA_TYPE == "datetime")
                        {
                            sqlType = "[datetime]";
                        }
                        else if (column.DATA_TYPE == "decimal")
                        {
                            sqlType = "[decimal](18, 2)";
                        }
                        else if (column.DATA_TYPE == "numeric")
                        {
                            sqlType = "[decimal](18, 2)";
                        }
                        else if (column.DATA_TYPE == "uniqueidentifier")
                        {
                            sqlType = "[uniqueidentifier]";
                        }
                        else if (column.DATA_TYPE == "int")
                        {
                            sqlType = "[int]";
                        }
                        else if (column.DATA_TYPE == "nchar")
                        {
                            var length = column.CHARACTER_MAXIMUM_LENGTH.HasValue ? column.CHARACTER_MAXIMUM_LENGTH.Value : 8000;
                            if (length > 4000)
                            {
                                sqlType = "[nchar](max)";
                                Console.WriteLine(tableName + "表的列" + column.COLUMN_NAME + "nchar(max).请检查是否合理");
                            }
                            else if (length == -1)
                            {
                                sqlType = "[nchar](max)";
                            }
                            else
                            {
                                sqlType = "[nchar](" + length + ")";
                            }

                        }
                        else if (column.DATA_TYPE == "nvarchar" || column.DATA_TYPE == "varchar")
                        {
                            var length = column.CHARACTER_MAXIMUM_LENGTH.HasValue ? column.CHARACTER_MAXIMUM_LENGTH.Value : 8000;
                            if (length > 4000)
                            {
                                sqlType = "[nvarchar](max)";
                                Console.WriteLine(tableName + "表的列" + column.COLUMN_NAME + "长度为nvarchar(max).请检查是否合理");
                            }
                            else if (length == -1)
                            {
                                sqlType = "[nvarchar](max)";
                            }
                            else
                            {
                                sqlType = "[nvarchar](" + length + ")";
                            }
                            if (length > 4000)
                            {
                                hasText = true;
                            }
                        }
                        else
                        {
                            sqlType = "[" + column.DATA_TYPE + "]";
                        }
                        sb1.Append(string.Format(" [{0}] {1} {2} NULL,", column.COLUMN_NAME, sqlType, column.IS_NULLABLE.ToUpper() == "YES" ? "" : "Not"));
                        sb1.Append(string.Format(" [{0}1] {1} {2} NULL,", column.COLUMN_NAME, sqlType, column.IS_NULLABLE.ToUpper() == "YES" ? "" : "Not"));
                    }
                }
                historySql = string.Format(historySql, tableName, sb1.ToString(), hasText ? "TEXTIMAGE_ON [PRIMARY]" : "", "NOT");
                try
                {
                    db.Execute(historySql);
                    Console.WriteLine("创建历史记录表成功 TABLE : " + tableName);
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
        /// <summary>
        /// 删除所有触发器
        /// </summary>
        /// <param name="db"></param>
        /// <param name="tableNames"></param>
        public static void DeleteTrigger(IDbConnection db, IEnumerable<string> tableNames)
        {
            var sql = "select name from sysobjects where type ='tr' order by name";
            var triggers = db.Query<string>(sql);
            foreach (var trigger in triggers)
            {
                try
                {
                    var dropSql = @"drop trigger {0}";
                    dropSql = string.Format(dropSql, trigger);
                    db.Execute(dropSql);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    continue;
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
            var columnAll = GetColumnNames(db);
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
                                                 INSERT INTO {0}History([F_Id],{1},[OperateTime],[Aop], [IsHand],[HandPC])
                                                 SELECT [F_Id],{2},GetDate() as [OperateTime],'Add' as Aop,0 as [IsHand],'' as [HandPC] from inserted
                                                 END
                                                 ELSE
                                                 BEGIN
                                                 DECLARE @hostname nvarchar(200)
                                                 SELECT @hostname=hostname FROM Master..SysProcesses WHERE Spid = @@spid
                                                 INSERT INTO {0}History
                                                 ([F_Id],{1},[OperateTime], [Aop], [IsHand],[HandPC])
                                                 SELECT [F_Id],{2},
                                                 GetDate() as [OperateTime],'Add' as Aop,1 as [IsHand],@hostname as [HandPC] 
                                                 from inserted
                                                 END
                                                 END";
                var insertTag = new List<string>();
                var selectTag = new List<string>();
                var columns = columnAll.Where(r => r.TABLE_NAME.ToUpper() == tableName.ToUpper());
                foreach (var colDoc in columns)
                {
                    if (colDoc.COLUMN_NAME != "F_Id" &&
                        colDoc.COLUMN_NAME != "F_CreateTime" &&
                        colDoc.COLUMN_NAME != "F_ModifyTime" &&
                        colDoc.COLUMN_NAME != "F_CreateUserID" &&
                        colDoc.COLUMN_NAME != "F_ModifyUserId")
                    {
                        insertTag.Add(string.Format("[{0}]", colDoc.COLUMN_NAME));
                        insertTag.Add(string.Format("[{0}1]", colDoc.COLUMN_NAME));
                        selectTag.Add(string.Format("[{0}]", colDoc.COLUMN_NAME));
                        selectTag.Add(string.Format("[{0}] as [{0}1]", colDoc.COLUMN_NAME));
                    }
                }
                try
                {
                    insertTrigger = string.Format(insertTrigger, tableName, string.Join(",", insertTag), string.Join(",", selectTag));
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
            var columnAll = GetColumnNames(db);
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
                                                    INSERT INTO {0}History([F_Id],{1},[OperateTime],[Aop],[IsHand],[HandPC])
                                                    Select [F_Id],{2}
                                                    ,GETDATE() as OperateTime,'Delete' AS [Aop],0 AS [IsHand],'' AS [HandPC]
                                                    FROM deleted
                                                    END
                                                    ELSE
                                                    BEGIN
                                                    DECLARE @hostname nvarchar(200)
                                                    SELECT @hostname=hostname FROM Master..SysProcesses WHERE Spid = @@spid
                                                    INSERT INTO {0}History([F_Id],{1},[OperateTime],[Aop],[IsHand],[HandPC])
                                                    select [F_Id],{2},GETDATE() as [OperateTime],
                                                    'Delete' AS Aop,1 AS [IsHand],@hostname AS [HandPC]
                                                    FROM deleted
                                                    END
                                                    END";
                var insertTag = new List<string>();
                var selectTag = new List<string>();
                var columns = columnAll.Where(r => r.TABLE_NAME.ToUpper() == tableName.ToUpper());
                foreach (var colDoc in columns)
                {
                    if (colDoc.COLUMN_NAME != "F_Id" &&
                        colDoc.COLUMN_NAME != "F_CreateTime" &&
                        colDoc.COLUMN_NAME != "F_ModifyTime" &&
                        colDoc.COLUMN_NAME != "F_CreateUserID" &&
                        colDoc.COLUMN_NAME != "F_ModifyUserId")
                    {
                        insertTag.Add(string.Format("[{0}]", colDoc.COLUMN_NAME));
                        insertTag.Add(string.Format("[{0}1]", colDoc.COLUMN_NAME));
                        selectTag.Add(string.Format("[{0}]", colDoc.COLUMN_NAME));
                        selectTag.Add(string.Format("[{0}] as [{0}1]", colDoc.COLUMN_NAME));
                    }
                }
                try
                {
                    deleteTrigger = string.Format(deleteTrigger, tableName, string.Join(",", insertTag), string.Join(",", selectTag));
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
            var columnAll = GetColumnNames(db);
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
                                                   	INSERT INTO [dbo].[{0}History]([F_Id],{1},[OperateTime],[Aop],[IsHand],[HandPC])
                                                   SELECT [F_Id],{2},GetDate() as [OperateTime],
                                                   		'Update' AS Aop,
                                                   		0 AS [IsHand],
                                                   		'' AS [HandPC]
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
                                                   INSERT INTO [dbo].[{0}History]([F_Id],{1},[OperateTime],[Aop],[IsHand],[HandPC])
                                                   SELECT [F_Id],{2},GetDate() as [OperateTime],
                                                   		'Update' AS Aop,
                                                   		1 AS [IsHand],
                                                   		@hostname AS [HandPC]
                                                   	FROM
                                                   		inserted 
                                                   end
                                                   END";
                var insertTag = new List<string>();
                var selectTag = new List<string>();
                var columns = columnAll.Where(r => r.TABLE_NAME.ToUpper() == tableName.ToUpper());
                foreach (var colDoc in columns)
                {
                    if (colDoc.COLUMN_NAME != "F_Id" &&
                        colDoc.COLUMN_NAME != "F_CreateTime" &&
                        colDoc.COLUMN_NAME != "F_ModifyTime" &&
                        colDoc.COLUMN_NAME != "F_CreateUserID" &&
                        colDoc.COLUMN_NAME != "F_ModifyUserId")
                    {
                        insertTag.Add(string.Format("[{0}]", colDoc.COLUMN_NAME));
                        insertTag.Add(string.Format("[{0}1]", colDoc.COLUMN_NAME));
                        selectTag.Add(string.Format("[{0}]", colDoc.COLUMN_NAME));
                        selectTag.Add(string.Format("[{0}] as [{0}1]", colDoc.COLUMN_NAME));
                    }
                }
                try
                {
                    updateTrigger = string.Format(updateTrigger, tableName, string.Join(",", insertTag), string.Join(",", selectTag));
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
