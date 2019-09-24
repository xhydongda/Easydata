using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;

namespace Easydata.Engine
{
    // Store manages databases.
    public class Store
    {
        Dictionary<string, Database> databases;
        const string config_file = "store.json";
        const string default_dbname = "default";
        StoreInfo info = null;
        Database default_db;
        StoreState state;
        readonly object lockthis = new object();

        public Store(string path)
        {
            if (String.IsNullOrEmpty(path))
            {
                throw new ArgumentNullException("store path is empty!");
            }
            Path = path;
            databases = new Dictionary<string, Database>();
            load();
        }

        //从配置文件中加载数据库列表.
        private void load()
        {
            string str = Path + config_file;
            if (File.Exists(str))
            {
                using (StreamReader sr = new StreamReader(str))
                {
                    string json = sr.ReadToEnd();
                    info = JsonConvert.DeserializeObject<StoreInfo>(json);
                }
                foreach (DatabasePath db_path in info.Databases)
                {
                    Database db = new Database(db_path.Name, db_path.Path, this);
                    databases.Add(db_path.Name, db);
                    if (db_path.Name == default_dbname)
                        default_db = db;
                }
            }
            else
            {
                info = new StoreInfo();
            }
            if (default_db == null)
            {
                default_db = createDatabase(default_dbname);
            }
        }

        //创建数据库实例，采用默认路径.
        private Database createDatabase(string dbname)
        {
            return createDatabase(dbname, Path + dbname + "\\");
        }

        //创建数据库实例，同步info.
        private Database createDatabase(string dbname, string dbpath)
        {
            Database db = new Database(dbname, dbpath, this);
            databases[dbname] = db;
            info.Databases.Add(new DatabasePath() { Name = dbname, Path = dbpath });
            save();//save db
            return db;
        }

        //保存数据库列表到配置文件.
        private void save()
        {
            string str = Path + config_file;
            string jsonstring = JsonConvert.SerializeObject(info);
            if (!Directory.Exists(Path))
                Directory.CreateDirectory(Path);
            using (StreamWriter sr = new StreamWriter(str, false))
            {
                sr.Write(jsonstring);
            }
        }

        public ulong NewShardId()
        {
            lock (lockthis)
            {
                info.MaxShardId++;
                save();
                return info.MaxShardId;
            }
        }

        /// <summary>
        /// 获得根路径.
        /// </summary>
        public string Path { get; private set; }

        /// <summary>
        /// 创建数据库实例.
        /// </summary>
        /// <param name="dbname">实例名</param>
        /// <returns>是否成功</returns>
        public bool CreateDatabase(string dbname)
        {
            return CreateDatabase(dbname, Path + dbname + "\\");
        }

        /// <summary>
        /// 创建数据库实例.
        /// </summary>
        /// <param name="dbname">实例名</param>
        /// <param name="dbpath">路径</param>
        /// <returns>是否成功</returns>
        public bool CreateDatabase(string dbname, string dbpath)
        {
            lock (lockthis)
            {
                if (String.IsNullOrEmpty(dbname) || String.IsNullOrEmpty(dbpath))
                {
                    return false;
                }
                if (databases.ContainsKey(dbname))
                {
                    return false;
                }
                createDatabase(dbname, dbpath);
                return true;
            }
        }

        /// <summary>
        /// 修改数据库实例名.
        /// 数据库实例的路径不变.
        /// </summary>
        /// <param name="olddbname">原数据库实例名</param>
        /// <param name="newdbname">新数据库实例名</param>
        /// <returns>是否成功</returns>
        public bool ChangeDatabaseName(string olddbname, string newdbname)
        {
            if (String.IsNullOrEmpty(olddbname) || String.IsNullOrEmpty(newdbname))
            {
                return false;
            }
            lock (lockthis)
            {
                if (!databases.ContainsKey(olddbname) || databases.ContainsKey(newdbname))
                    return false;
                Database db = databases[olddbname];
                databases.Remove(olddbname);
                databases.Add(newdbname, db);
                db.Rename(newdbname);
                foreach (DatabasePath db_path in info.Databases)
                {
                    if (db_path.Name == olddbname)
                    {
                        db_path.Name = newdbname;
                        break;
                    }
                }
                save();
                return true;
            }
        }

        /// <summary>
        /// 删除数据库.
        /// </summary>
        /// <param name="name">数据库名</param>
        /// <returns>是否成功</returns>
        public bool DeleteDatabase(string name)
        {
            if (String.IsNullOrEmpty(name) || name == default_dbname)
                return false;
            lock (lockthis)
            {
                if (!databases.ContainsKey(name))
                    return false;
                Database db = databases[name];
                db.Delete();
                databases.Remove(name);
                DatabasePath todel = null;
                foreach (DatabasePath db_path in info.Databases)
                {
                    if (db_path.Name == name)
                    {
                        todel = db_path;
                        break;
                    }
                }
                if (todel != null)
                {
                    info.Databases.Remove(todel);
                }
                save();
                return true;
            }
        }

        /// <summary>
        /// 注册sids，只有注册后的数据点才能读写.
        /// </summary>
        /// <param name="sids">[Sid]</param>
        /// <param name="days">存储时限</param>
        public int AddSids(List<ulong> sids, int days)
        {
            return AddSids(default_dbname, sids, days);
        }

        /// <summary>
        /// 注册sids，只有注册后的数据点才能读写.
        /// </summary>
        /// <param name="database">数据库实例名</param>
        /// <param name="sids">[Sid]</param>
        /// <param name="days">存储时限</param>
        /// <returns>注册成功的sid数量</returns>
        public int AddSids(string database, List<ulong> sids, int days)
        {
            Database db = null;
            lock (lockthis)
            {
                if (databases.ContainsKey(database))
                {
                    db = databases[database];
                }
            }
            if (db != null)
            {
                return db.AddSids(sids, days);
            }
            return 0;
        }

        /// <summary>
        /// 删除sids注册和数据.
        /// </summary>
        /// <param name="sids">[Sid]</param>
        /// <returns>删除成功的sid数量</returns>
        public int RemoveSids(List<ulong> sids)
        {
            return RemoveSids(default_dbname, sids);
        }

        /// <summary>
        /// 删除sids注册和数据.
        /// </summary>
        /// <param name="database">数据库实例名</param>
        /// <param name="sids">[Sid]</param>
        /// <returns>删除成功的sid数量</returns>
        public int RemoveSids(string database, List<ulong> sids)
        {
            Database db = null;
            lock (lockthis)
            {
                if (databases.ContainsKey(database))
                {
                    db = databases[database];
                }
            }
            if (db != null)
            {
                return db.RemoveSids(sids);
            }
            return 0;
        }

        public void Open()
        {
            lock (lockthis)
            {
                if (state == StoreState.Opening || state == StoreState.Opened)
                {
                    return;
                }//正打开或已打开的返回.
                state = StoreState.Opening;
            }
            Logger.Info("开始启动引擎...");
            lock (lockthis)
            {
                foreach (Database db in databases.Values)
                {
                    db.Open();
                }
                state = StoreState.Opened;
            }
            Logger.Info("引擎启动完毕");
        }

        public StoreState State
        {
            get
            {
                lock (lockthis)
                {
                    return state;
                }
            }
        }

        public void Close()
        {
            lock (lockthis)
            {
                if (state == StoreState.Closing || state == StoreState.Closed)
                {
                    return;
                }//正关闭或已关闭的返回.
                state = StoreState.Closing;
            }
            Logger.Info("开始关闭引擎...");
            lock (lockthis)
            {
                foreach (Database db in databases.Values)
                {
                    db.Close();
                }
                state = StoreState.Closed;
            }
            Logger.Info("引擎关闭");
        }
        public void CloseDatabase(string dbname)
        {
            lock (lockthis)
            {
                if (databases.ContainsKey(dbname))
                {
                    databases[dbname].Close();
                }
            }
        }
        public void OpenDatabase(string dbname)
        {
            lock (lockthis)
            {
                if (databases.ContainsKey(dbname))
                {
                    databases[dbname].Open();
                }
            }
        }

        /// <summary>
        /// 数据库实例是否存在.
        /// </summary>
        /// <returns>数据库实例名</returns>
        public bool ContainDatabase(string dbname)
        {
            lock (lockthis)
            {
                return databases.ContainsKey(dbname);
            }
        }

        /// <summary>
        /// 获取数据库列表.
        /// </summary>
        /// <returns>数据库列表</returns>
        public List<string> Databases()
        {
            lock (lockthis)
            {
                return new List<string>(databases.Keys);
            }
        }

        /// <summary>
        /// 获取占用的磁盘空间.
        /// </summary>
        /// <returns>占用的磁盘空间</returns>
        public long DiskSize()
        {
            long result = 0;
            lock (lockthis)
            {
                foreach (Database db in databases.Values)
                {
                    result += db.DiskSize();
                }
            }
            return result;
        }

        /// <summary>
        /// 写入数据库.
        /// </summary>
        /// <param name="points">数据点列表</param>
        /// <returns>写入成功的点数</returns>
        public int Write(Dictionary<ulong, ClockValues> points)
        {
            if (state != StoreState.Opened || points == null || points.Count == 0)
                return 0;
            bool singledatabase = false;
            lock (lockthis)
            {
                singledatabase = (databases.Count == 1);
            }
            if (singledatabase)
            {
                return default_db.Write(points);
            }//只有默认数据库的直接写入.
            Dictionary<string, Dictionary<ulong, ClockValues>> database_points = new Dictionary<string, Dictionary<ulong, ClockValues>>();
            lock (lockthis)
            {
                foreach (Database db in databases.Values)
                {
                    foreach (KeyValuePair<ulong, ClockValues> sidvalues in points)
                    {
                        if (db.ContainsSid(sidvalues.Key))
                        {
                            string database = db.Name;
                            if (!database_points.ContainsKey(database))
                                database_points.Add(database, new Dictionary<ulong, ClockValues>());
                            database_points[database].Add(sidvalues.Key, sidvalues.Value);
                        }
                    }//按database分组
                }
            }
            int result = 0;
            foreach (KeyValuePair<string, Dictionary<ulong, ClockValues>> pair in database_points)
            {
                Database db = null;
                lock (lockthis)
                {
                    if (databases.ContainsKey(pair.Key))
                    {
                        db = databases[pair.Key];
                    }
                    else
                    {
                        continue;
                    }
                }
                result += db.Write(pair.Value);
            }//写入对应数据库
            return result;
        }

        /// <summary>
        /// 写入某数据库.
        /// </summary>
        /// <param name="database">数据库实例名</param>
        /// <param name="points">数据点列表</param>
        /// <returns>写入成功的点数</returns>
        public int Write(string database, Dictionary<ulong, ClockValues> points)
        {
            if (state != StoreState.Opened || points == null || points.Count == 0)
                return 0;
            Database db = null;
            lock (lockthis)
            {
                if (databases.ContainsKey(database))
                {
                    db = databases[database];
                }
            }
            if (db != null)
                return db.Write(points);

            return 0;
        }


        /// <summary>
        /// 读取数据.
        /// </summary>
        /// <param name="sids">数据点列表</param>
        /// <param name="start">开始时间</param>
        /// <param name="end">结束时间</param>
        /// <returns>读到的数据</returns>
        public Dictionary<ulong, ClockValues> Read(List<ulong> sids, long start, long end)
        {
            if (state != StoreState.Opened || sids == null || sids.Count == 0)
                return null;
            bool singledatabase = false;
            lock (lockthis)
            {
                singledatabase = (databases.Count == 1);
            }
            if (singledatabase)
            {
                return default_db.Read(sids, start, end);
            }//只有默认数据库的直接读取.
            Dictionary<string, List<ulong>> database_sids = new Dictionary<string, List<ulong>>();
            lock (lockthis)
            {
                foreach (Database db in databases.Values)
                {
                    foreach (ulong sid in sids)
                    {
                        if (db.ContainsSid(sid))
                        {
                            string database = db.Name;
                            if (!database_sids.ContainsKey(database))
                                database_sids.Add(database, new List<ulong>());
                            database_sids[database].Add(sid);
                        }
                    }
                }//按database分组
            }
            Dictionary<ulong, ClockValues> result = new Dictionary<ulong, ClockValues>();
            foreach (KeyValuePair<string, List<ulong>> pair in database_sids)
            {
                Database db = null;
                lock (lockthis)
                {
                    if (databases.ContainsKey(pair.Key))
                    {
                        db = databases[pair.Key];
                    }
                    else
                    {
                        continue;
                    }
                }
                Dictionary<ulong, ClockValues> lst = db.Read(pair.Value, start, end);
                if (lst != null)
                {
                    foreach (KeyValuePair<ulong, ClockValues> item in lst)
                    {
                        if (!result.ContainsKey(item.Key))
                            result.Add(item.Key, item.Value);
                        else
                            result[item.Key].AddRange(item.Value);
                    }
                }
            }//写入对应数据库
            return result;
        }

        /// <summary>
        /// 读取某数据库的数据.
        /// </summary>
        /// <param name="database">数据库实例名</param>
        /// <param name="sids">数据点列表</param>
        /// <param name="start">开始时间</param>
        /// <param name="end">结束时间</param>
        /// <returns>读到的数据</returns>
        public Dictionary<ulong, ClockValues> Read(string database, List<ulong> sids, long start, long end)
        {
            if (state != StoreState.Opened || sids == null || sids.Count == 0)
                return null;

            Database db = null;
            lock (lockthis)
            {
                if (databases.ContainsKey(database))
                {
                    db = databases[database];
                }
            }
            return db.Read(sids, start, end);
        }
    }

    public class StoreInfo
    {
        public StoreInfo()
        {
            Databases = new List<DatabasePath>();
        }

        public List<DatabasePath> Databases { get; set; }

        public ulong MaxShardId { get; set; }
    }

    public class DatabasePath
    {
        public DatabasePath()
        { }

        public string Name { get; set; }

        public string Path { get; set; }
    }

    /// <summary>
    /// 数据库状态.
    /// </summary>
    public enum StoreState
    {
        /// <summary>
        /// 正在打开.
        /// </summary>
        Opening = 1,
        /// <summary>
        /// 已打开.
        /// </summary>
        Opened = 2,
        /// <summary>
        /// 正在关闭.
        /// </summary>
        Closing = 4,
        /// <summary>
        /// 已关闭.
        /// </summary>
        Closed = 8
    }
}
