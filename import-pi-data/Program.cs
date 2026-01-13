using Apache.IoTDB;
using Apache.IoTDB.DataStructure;
using NLog.Filters;
using PISDK;
using PISDKCommon;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Script.Serialization;

namespace import_pi_data
{
    public class AppConfig
    {
        public PiServerConfig PiServer { get; set; }
        public IoTDBConfig IoTDB { get; set; }
        public DataRangeConfig DataRange { get; set; }
        public PerformanceConfig Performance { get; set; }
    }

    public class PiServerConfig
    {
        public string Address { get; set; }
    }

    public class IoTDBConfig
    {
        public string Host { get; set; }
        public int Port { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public string RootDevice { get; set; }
    }

    public class DataRangeConfig
    {
        public string StartTime { get; set; }
        public string EndTime { get; set; }
    }

    public class PerformanceConfig
    {
        public int ParallelTaskCount { get; set; }
        public int MaxRetry { get; set; }
    }

    internal static class PiToIoTDB_StreamReadWrite
    {
        private static AppConfig _config;
        private static string piServerAddress;
        private static string iotdbHost;
        private static int iotdbPort;
        private static string iotdbUser;
        private static string iotdbPwd;
        private static string iotdbRootDevice;
        private static int parallelTaskCount;
        private static int maxRetry;
        private static DateTime dataStartTime;
        private static DateTime dataEndTime;
        private static bool stop = false;
        private static int valuesCnt = 0;
        private static int faileCnt = 0;
        private static int totalValuesCnt = 0;

        static void LoadConfiguration()
        {
            try
            {
                string configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "appsettings.json");
                if (!File.Exists(configPath))
                {
                    configPath = "appsettings.json";
                }

                string json = File.ReadAllText(configPath);
                var serializer = new JavaScriptSerializer();
                _config = serializer.Deserialize<AppConfig>(json);

                piServerAddress = _config.PiServer.Address;
                iotdbHost = _config.IoTDB.Host;
                iotdbPort = _config.IoTDB.Port;
                iotdbUser = _config.IoTDB.User;
                iotdbPwd = _config.IoTDB.Password;
                iotdbRootDevice = _config.IoTDB.RootDevice;
                dataStartTime = DateTime.Parse(_config.DataRange.StartTime);
                dataEndTime = DateTime.Parse(_config.DataRange.EndTime);
                parallelTaskCount = _config.Performance.ParallelTaskCount > 0 ? _config.Performance.ParallelTaskCount : Environment.ProcessorCount * 2;
                maxRetry = _config.Performance.MaxRetry;

                Console.WriteLine("配置加载成功");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"配置加载失败：{ex.Message}");
                throw;
            }
        }

        // 复用PI连接（全局单例，避免重复创建）
        private static Server _piServer;
        private static readonly object _piLock = new object();

        static async Task Main(string[] args)
        {
            LoadConfiguration();
            
            var totalWatch = Stopwatch.StartNew();
            ConcurrentQueue<PiPointInfo> allPoints = new ConcurrentQueue<PiPointInfo>();
            try
            {
                Console.WriteLine($"时间范围：{dataStartTime:yyyy-MM-dd HH:mm:ss} 至 {dataEndTime:yyyy-MM-dd HH:mm:ss}");

                // 步骤1：读取所有点位元数据（仅读元数据，不读历史数据，内存占用低）
                Console.WriteLine("\n----- 步骤1：读取点位元数据 -----");
                Console.WriteLine($"开始时间{DateTime.Now.ToLocalTime()}");
                Task.Run(async () =>
                {
                    ReadPointMetadata(allPoints);
                });
                // 步骤2：边读边写（核心逻辑）
                Console.WriteLine("\n----- 步骤2：流式读写历史数据 -----");
                await StreamProcess(allPoints);
                Console.WriteLine($"共读取{valuesCnt}条数据");
                totalWatch.Stop();
                Console.WriteLine($"\n===== 全部完成，总耗时：{totalWatch.Elapsed.TotalMinutes:F2}分钟 =====");
            }
            catch (Exception ex)
            {
                totalWatch.Stop();
                Console.WriteLine($"失败（总耗时：{totalWatch.Elapsed.TotalMinutes:F2}分钟）：{ex.Message}");
            }
            finally
            {
                // 释放PI连接
                if (_piServer != null && _piServer.Connected)
                {
                    _piServer.Close();
                    Console.WriteLine("PI连接已关闭");
                }
            }
            Console.WriteLine("按任意键退出...");
            Console.ReadKey();
        }

        #region 步骤1：读取点位元数据
        static void ReadPointMetadata(ConcurrentQueue<PiPointInfo> result)
        {
            var watch = Stopwatch.StartNew();
            int count = 0;

            try
            {
                _piServer = GetPiServer();
                PIPoints piPoints = _piServer.PIPoints;
                IEnumerator enumerator = piPoints.GetEnumerator();
                while (enumerator.MoveNext())
                {
                    PIPoint point = (PIPoint)enumerator.Current;
                    if (point == null)
                        continue;
                    result.Enqueue(new PiPointInfo
                    {
                        RawName = point.Name,
                        PiDataType = point.PointType.ToString()
                    });
                    count++;
                    if (count % 1000 == 0)
                    {
                        Console.WriteLine($"已读取{count:N0}个点位元数据...");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"元数据读取失败：{ex.Message}");
            }
            finally
            {
                watch.Stop();
                Console.WriteLine($"元数据读取耗时：{watch.Elapsed.TotalMinutes:F2}分钟");
            }
            stop = true;
            return;  // 一次性返回所有结果
        }
        #endregion

        static Server GetPiServer()
        {
            if (_piServer == null || !_piServer.Connected)
            {
                lock (_piLock)
                {
                    if (_piServer == null || !_piServer.Connected)
                    {
                        var watch = Stopwatch.StartNew();
                        var sdk = new PISDK.PISDK();
                        _piServer = sdk.Servers[piServerAddress];

                        _piServer.Open();
                        watch.Stop();
                        Console.WriteLine($"PI连接成功，耗时：{watch.Elapsed.TotalSeconds:F2}秒");
                    }
                }
            }
            return _piServer;
        }

        #region 步骤2：边读边写核心逻辑（单点位处理完立即释放内存）
        static async Task StreamProcess(ConcurrentQueue<PiPointInfo> allPoints)
        {
            // 创建IoTDB连接池（复用）
            using (var iotdbPool = new SessionPool(iotdbHost, iotdbPort, iotdbUser, iotdbPwd, 2048))
            {
                await iotdbPool.Open(false);
                Console.WriteLine("IoTDB连接池开启成功");
                // 信号量控制并行任务数（避免内存激增）
                using (var semaphore = new SemaphoreSlim(parallelTaskCount))
                {
                    var tasks = new List<Task>();
                    int total = 0;
                    int completed = 0;
                    //bool bbb = true;
                    while (allPoints.TryDequeue(out PiPointInfo point) || !stop)
                    {
                        if (point != null)
                        {
                            total++;
                            await semaphore.WaitAsync(); // 控制并行数量

                            // 每个点位单独创建任务
                            tasks.Add(Task.Run(async () =>
                            {
                                try
                                {
                                    // 单个点位：读取→写入→释放内存
                                    await ProcessSinglePoint(point, iotdbPool);
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"{DateTime.Now.ToLocalTime()} : {ex.Message}」");
                                }
                                finally
                                {
                                    // 原子操作更新进度
                                    int current = Interlocked.Increment(ref completed);
                                    if (current % 100 == 0) // 每完成100个点位输出进度
                                    {
                                        Console.WriteLine($"{DateTime.Now.ToLocalTime()}已完成{current:N0}/{total:N0}个点位，内存占用稳定");
                                    }
                                    semaphore.Release();
                                }
                            }));
                        }
                        else
                        {
                            Thread.Sleep(1000);
                        }
                    }
                    // 等待所有点位处理完成
                    await Task.WhenAll(tasks);
                }
            }
        }
        // 处理单个点位
        static async Task ProcessSinglePoint(PiPointInfo point, SessionPool iotdbPool)
        {
            // 仅缓存当前点位的数据（读取后立即写入，写入后释放）
            List<long> timestamps = null;
            List<object> values = null;
            try
            {
                // 1. 读取当前点位的历史数据（仅加载当前点位数据到内存）
                (timestamps, values) = await ReadSinglePointData(point);
                // 
                if (timestamps == null || timestamps.Count == 0)
                {
                    ;
                    return;
                }
                var value_lst = new List<List<object>>();
                for (int i = 0; i < timestamps.Count; i++)
                {
                    var value = new List<object> { values[i] };
                    value_lst.Add(value);
                }
                var tablet = new Tablet(
                    deviceId: iotdbRootDevice,
                    measurements: new List<string> { $"`{point.RawName}`" },
                    values: value_lst,
                    timestamps: timestamps
                );

                await iotdbPool.InsertTabletAsync(tablet);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now.ToLocalTime()} 点位[{point.RawName}]未成功写入.记录日志{ex.Message}」");

            }
            finally
            {
                // 显式释放内存（帮助GC回收）
                timestamps?.Clear();
                values?.Clear();
                timestamps = null;
                values = null;
            }
        }
        #endregion

        #region 读取单个点位数据（仅加载当前点位数据）
        static async Task<(List<long> timestamps, List<object> values)> ReadSinglePointData(PiPointInfo point)
        {
            // 仅为当前点位创建内存空间
            var timestamps = new List<long>();
            var values = new List<object>();

            try
            {
                // 复用PI连接，获取点位
                var piServer = GetPiServer();
                var piPoint = piServer.PIPoints[point.RawName];
                var piData = piPoint.Data;


                var watch = new Stopwatch();
                // 第一段任务
                watch.Start();
                PIValues pvs = piData.RecordedValues(
                    dataStartTime, dataEndTime,
                    BoundaryTypeConstants.btInside
                );
                // 处理数据（区分Digital类型）
                bool isDigital = point.PiDataType.IndexOf("pttypdigital", StringComparison.OrdinalIgnoreCase) >= 0;

                foreach (PIValue pv in pvs)
                {

                    // 数据值：Digital取Code
                    if (isDigital)
                    {
                        values.Add(((DigitalState)pv.Value).Code);
                        valuesCnt++;
                        // 时间戳：Unix毫秒
                        DateTime time = pv.TimeStamp.LocalDate;
                        timestamps.Add(new DateTimeOffset(time).ToUnixTimeMilliseconds());

                    }
                    else
                    {

                        if (!(pv.Value is DigitalState ds))
                        {
                            values.Add(pv.Value);
                            valuesCnt++;
                            DateTime time = pv.TimeStamp.LocalDate;
                            timestamps.Add(new DateTimeOffset(time).ToUnixTimeMilliseconds());
                        }
                    }
                }
                watch.Stop();
                return (timestamps, values);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  点位[{point.RawName}]读取失败：{ex.Message}");

                faileCnt++;
                timestamps?.Clear();
                values?.Clear();
                return (null, null);
            }
        }
        #endregion
        // 点位元数据类（仅包含必要信息，内存占用小）
        public class PiPointInfo
        {
            public string RawName { get; set; }
            public string PiDataType { get; set; }
        }
    }
}



