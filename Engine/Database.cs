using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;

namespace Easydata.Engine
{
    /// <summary>
    /// 数据库实例.
    /// </summary>
    public class Database
    {
        string _Path, _Name;
        const string config_file = "db.json";
        DatabaseInfo info = null;
        Store _Store;
        readonly object lockthis = new object();
        DatabaseState _State = DatabaseState.Closed;
        Dictionary<int, RP> _RPs;

        /// <summary>
        /// 新建数据库实例.
        /// </summary>
        /// <param name="name">数据库实例名称</param>
        /// <param name="path">存储路径</param>
        /// <param name="store">引擎</param>
        public Database(string name, string path, Store store)
        {
            _Name = name;
            _Path = path;
            _Store = store;
            _RPs = new Dictionary<int, RP>();
            load();
        }

        /// <summary>
        /// 获取Store实例.
        /// </summary>
        public Store Store { get { return _Store; } }

        //从配置文件中加载rp列表.
        private void load()
        {
            string str = _Path + config_file;
            if (File.Exists(str))
            {
                using (StreamReader sr = new StreamReader(str))
                {
                    string json = sr.ReadToEnd();
                    info = JsonConvert.DeserializeObject<DatabaseInfo>(json);
                }
                foreach (int days in info.RPs)
                {
                    _RPs[days] = new RP(days, this);
                }
            }
            else
            {
                info = new DatabaseInfo();
            }
        }

        //保存rp列表到配置文件.
        private void save()
        {
            string str = _Path + config_file;
            string jsonstring = JsonConvert.SerializeObject(info);
            if (!Directory.Exists(_Path))
                Directory.CreateDirectory(_Path);
            using (StreamWriter sr = new StreamWriter(str, false))
            {
                sr.Write(jsonstring);
            }
        }

        #region 数据库管理

        /// <summary>
        /// 重命名数据库实例名.
        /// </summary>
        /// <param name="newname">新实例名称</param>
        public void Rename(string newname)
        {
            Close();
            lock (lockthis)
            {
                _Name = newname;
            }
            Open();
        }

        /// <summary>
        /// 打开数据库.
        /// </summary>
        public void Open()
        {
            lock (lockthis)
            {
                if (_State == DatabaseState.Opening || _State == DatabaseState.Opened)
                {
                    return;
                }//正打开或已打开的返回.
                _State = DatabaseState.Opening;
            }
            Logger.Info(string.Format("开始打开数据库: {0}", _Name));
            lock (lockthis)
            {
                foreach (RP rp in _RPs.Values)
                {
                    rp.Open();
                }
                _State = DatabaseState.Opened;
            }
            Logger.Info(string.Format("数据库: {0} 已打开", _Name));
        }


        /// <summary>
        /// 注册sids，只有注册后的数据点才能读写.
        /// </summary>
        /// <param name="sids">[Sid]</param>
        /// <param name="days">存储时限</param>
        /// <returns>注册成功的sid数量</returns>
        public int AddSids(List<ulong> sids, int days)
        {
            lock (lockthis)
            {
                if (_RPs.ContainsKey(days))
                {
                    return _RPs[days].AddSids(sids);
                }
                else
                {
                    RP newrp = new RP(days, this);
                    _RPs[days] = newrp;
                    info.RPs.Add(days);
                    save();
                    return newrp.AddSids(sids);
                }
            }
        }

        /// <summary>
        /// 删除sids注册和数据.
        /// </summary>
        /// <param name="sids">[Sid]</param>
        /// <returns>删除成功的sid数量</returns>
        public int RemoveSids(List<ulong> sids)
        {
            lock (lockthis)
            {
                int result = 0;
                foreach (RP rp in _RPs.Values)
                {
                    result += rp.RemoveSids(sids);
                }
                return result;
            }
        }

        /// <summary>
        /// 是否有某sid.
        /// </summary>
        /// <param name="sid">sid</param>
        /// <returns>true=有</returns>
        public bool ContainsSid(ulong sid)
        {
            lock (lockthis)
            {
                foreach (RP rp in _RPs.Values)
                {
                    if (rp.ContainsSid(sid))
                        return true;
                }
            }
            return false;
        }

        /// <summary>
        /// 关闭数据库实例，步骤:
        /// 1.使数据库处于关闭状态;
        /// 2.释放所有打开的文件Stream;
        /// 3.将索引写入到文件中.
        /// </summary>
        public void Close()
        {
            lock (lockthis)
            {
                if (_State == DatabaseState.Closing || _State == DatabaseState.Closed)
                {
                    return;
                }//正关闭或已关闭的返回.
                _State = DatabaseState.Closing;
            }
            Logger.Info(string.Format("开始关闭数据库: {0}", _Name));
            lock (lockthis)
            {
                foreach (RP rp in _RPs.Values)
                {
                    rp.Close();
                }
                _State = DatabaseState.Closed;
            }
            Logger.Info(string.Format("数据库: {0} 已关闭", _Name));
        }
        /// <summary>
        /// 删除数据库实例，步骤:
        /// 1.关闭数据库;
        /// 2.删除文件夹.
        /// </summary>
        public void Delete()
        {
            Logger.Info(string.Format("开始删除数据库: {0}", _Name));
            Close();
            lock (lockthis)
            {
                Directory.Delete(_Path);
                _State = DatabaseState.Deleted;
            }
            Logger.Info(string.Format("数据库: {0} 已删除", _Name));
        }

        public void Backup(string tofile)
        {
            Logger.Info(string.Format("开始备份数据库: {0} 到 {1}", _Name, tofile));

            Logger.Info(string.Format("数据库: {0} 备份完毕", _Name));
        }

        public void Restore(string fromfile)
        {
            Logger.Info(string.Format("开始从 {0} 恢复数据库: {1}", fromfile, _Name));

            Logger.Info(string.Format("数据库: {0} 恢复完毕", _Name));
        }

        #endregion

        #region 数据库查询

        public string Path
        {
            get
            {
                lock (lockthis)
                {
                    return _Path;
                }
            }
        }

        public string Name
        {
            get
            {
                lock (lockthis)
                {
                    return _Name;
                }
            }
        }

        public long DiskSize()
        {
            long result = 0;
            lock (lockthis)
            {
                foreach (RP rp in _RPs.Values)
                {
                    result += rp.DiskSize();
                }
            }
            return result;
        }

        public DatabaseState State
        {
            get
            {
                lock (lockthis)
                {
                    return _State;
                }
            }
        }
        #endregion


        public int Write(Dictionary<ulong, ClockValues> points)
        {
            if (_State != DatabaseState.Opened || points == null || points.Count == 0)
                return 0;
            Dictionary<int, Dictionary<ulong, ClockValues>> rp_points = new Dictionary<int, Dictionary<ulong, ClockValues>>();
            lock (lockthis)
            {
                foreach (KeyValuePair<int, RP> pair in _RPs)
                {
                    foreach (KeyValuePair<ulong, ClockValues> sidvalues in points)
                    {
                        if (pair.Value.ContainsSid(sidvalues.Key))
                        {
                            int days = pair.Key;
                            if (!rp_points.ContainsKey(days))
                                rp_points.Add(days, new Dictionary<ulong, ClockValues>());
                            rp_points[days].Add(sidvalues.Key, sidvalues.Value);
                        }
                    }//按rp分组
                }
            }
            int result = 0;
            foreach (KeyValuePair<int, Dictionary<ulong, ClockValues>> pair in rp_points)
            {
                RP rp = null;
                lock (lockthis)
                {
                    rp = _RPs[pair.Key];
                }
                result += rp.Write(pair.Value);
            }//写入对应rp
            return result;
        }


        public Dictionary<ulong, ClockValues> Read(List<ulong> sids, long start, long end)
        {
            if (_State != DatabaseState.Opened || sids == null || sids.Count == 0)
                return null;
            Dictionary<int, List<ulong>> rp_sids = new Dictionary<int, List<ulong>>();
            lock (lockthis)
            {
                foreach (KeyValuePair<int, RP> pair in _RPs)
                {
                    foreach (ulong sid in sids)
                    {
                        if (pair.Value.ContainsSid(sid))
                        {
                            int days = pair.Key;
                            if (!rp_sids.ContainsKey(days))
                                rp_sids.Add(days, new List<ulong>());
                            rp_sids[days].Add(sid);
                        }
                    }//按rp分组
                }
            }
            Dictionary<ulong, ClockValues> result = new Dictionary<ulong, ClockValues>();
            foreach (KeyValuePair<int, List<ulong>> pair in rp_sids)
            {
                RP rp = null;
                lock (lockthis)
                {
                    if (_RPs.ContainsKey(pair.Key))
                    {
                        rp = _RPs[pair.Key];
                    }
                    else
                    {
                        continue;
                    }
                }
                Dictionary<ulong, ClockValues> lst = rp.Read(pair.Value, start, end);
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
            }//写入对应rp
            return result;
        }
    }

    public class DatabaseInfo
    {
        public DatabaseInfo()
        {
            RPs = new List<int>();
        }

        public List<int> RPs { get; set; }
    }

    /// <summary>
    /// 数据库状态.
    /// </summary>
    public enum DatabaseState
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
        Closed = 8,
        /// <summary>
        /// 已删除.
        /// </summary>
        Deleted = 16
    }
}
