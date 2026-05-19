using Apache.IoTDB;
using PISDK;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace import_pi_data
{
    public static class PiPointMigration
    {
        private static string piServerAddress = "RTDB";
        private static string iotdbHost = "127.0.0.1";
        private static int iotdbPort = 6667;
        private static string iotdbUser = "root";
        private static string iotdbPwd = "TimechoDB@2021";
        private static string iotdbRootDevice = "root.pi.test";
        private static int batchSize = 1000;

        public static async Task RunMigrationAsync()
        {
            try
            {
                Console.WriteLine("===== 从PI同步点位并创建IoTDB时序（仅保留描述属性） =====");
                DateTime strat = DateTime.Now;
                Console.WriteLine($"读取开始时间：{strat}");

                var piPointBatches = GetPiPointMetadataBatches();

                DateTime end = DateTime.Now;
                Console.WriteLine($"读取结束时间：{end}");

                int totalBatches = piPointBatches.Count;

                DateTime strat2 = DateTime.Now;
                Console.WriteLine($"写入开始时间：{strat2}");

                await BatchCreateTimeseries(piPointBatches);

                DateTime end1 = DateTime.Now;
                Console.WriteLine($"写入结束时间：{end1}");
                Console.WriteLine("===== 点位迁移完成 =====");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"点位迁移执行失败：{ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"内部错误：{ex.InnerException.Message}");
                throw;
            }
        }

        static List<List<PiPointInfo>> GetPiPointMetadataBatches()
        {
            var batches = new List<List<PiPointInfo>>();
            var currentBatch = new List<PiPointInfo>();
            PISDK.PISDK piSdk = new PISDK.PISDK();
            Server piServer = null;
            IEnumerator pointEnumerator = null;

            try
            {
                piServer = piSdk.Servers[piServerAddress];
                piServer.Open();
                Console.WriteLine($"已连接PI服务器：{piServerAddress}");

                PIPoints piPoints = piServer.PIPoints;
                pointEnumerator = piPoints.GetEnumerator();
                int totalCount = 0;

                while (pointEnumerator.MoveNext())
                {
                    PIPoint piPoint = (PIPoint)pointEnumerator.Current;
                    if (piPoint == null) continue;

                    var pointInfo = new PiPointInfo
                    {
                        PointName = piPoint.Name,
                        PiDataType = piPoint.PointType.ToString(),
                        Description = piPoint.PointAttributes["descriptor"].Value
                    };

                    currentBatch.Add(pointInfo);
                    totalCount++;

                    if (currentBatch.Count >= batchSize)
                    {
                        batches.Add(currentBatch);
                        currentBatch = new List<PiPointInfo>();
                        Console.WriteLine($"已读取{totalCount}个PI点位...");
                    }
                }

                if (currentBatch.Count > 0)
                {
                    batches.Add(currentBatch);
                    currentBatch = new List<PiPointInfo>();
                    Console.WriteLine($"已读取{totalCount}个PI点位...");
                }

                Console.WriteLine($"PI点位读取完成，共{totalCount}个有效点位");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"PI读取失败：{ex.Message}");
            }
            finally
            {
                if (piServer != null)
                {
                    Console.WriteLine("关闭piserver");
                    piServer.Close();
                }
            }

            return batches;
        }

        static async Task BatchCreateTimeseries(List<List<PiPointInfo>> pointBatches)
        {
            var sessionPool = new SessionPool(iotdbHost, iotdbPort, iotdbUser, iotdbPwd, 1024);

            try
            {
                await sessionPool.Open(false);
                Console.WriteLine("IoTDB连接池开启成功");

                for (int i = 0; i < pointBatches.Count; i++)
                {
                    var currentBatch = pointBatches[i];
                    int batchNumber = i + 1;
                    Console.WriteLine($"处理第{batchNumber}批，共{currentBatch.Count}个时序...");

                    var tsPaths = new List<string>();
                    var dataTypes = new List<TSDataType>();
                    var encodings = new List<TSEncoding>();
                    var compressors = new List<Compressor>();
                    var attributeSqls = new List<string>();

                    foreach (var pointInfo in currentBatch)
                    {
                        string timeseriesPath = $"{iotdbRootDevice}.`{pointInfo.PointName}`";

                        TSDataType iotdbType = MapPiTypeToIoTDBType(pointInfo.PiDataType);
                        TSEncoding encoding = GetEncodingByType(iotdbType);
                        Compressor compressor = Compressor.SNAPPY;

                        string description = pointInfo.Description.Replace("'", "''");

                        tsPaths.Add(timeseriesPath);
                        dataTypes.Add(iotdbType);
                        encodings.Add(encoding);
                        compressors.Add(compressor);

                        string setAttrSql = $"ALTER TIMESERIES {timeseriesPath} ADD ATTRIBUTES 'description' = '{description}'";
                        attributeSqls.Add(setAttrSql);
                    }

                    Console.WriteLine("开始执行！");

                    await sessionPool.CreateMultiTimeSeriesAsync(
                        tsPaths,
                        dataTypes,
                        encodings,
                        compressors
                    );

                    foreach (var sql in attributeSqls)
                    {
                        await sessionPool.ExecuteNonQueryStatementAsync(sql);
                    }

                    Console.WriteLine("完成插入！");
                }
            }
            finally
            {
                if (sessionPool.IsOpen())
                {
                    await sessionPool.Close();
                    Console.WriteLine("IoTDB连接池已关闭");
                }
            }
        }

        static TSDataType MapPiTypeToIoTDBType(string piDataType)
        {
            if (piDataType == null) return TSDataType.FLOAT;
            String lowerType = piDataType.ToLower();

            if (lowerType.Contains("pttypint32") || lowerType.Contains("pttypdigital") || lowerType.Contains("pttypint16"))
            {
                return TSDataType.INT32;
            }
            else if (lowerType.Contains("pttypstring") || lowerType.Contains("pttyptext"))
            {
                return TSDataType.STRING;
            }
            else if (lowerType.Contains("pttypblob"))
            {
                return TSDataType.BLOB;
            }
            else if (lowerType.Contains("pttyptimestamp"))
            {
                return TSDataType.TIMESTAMP;
            }
            else if (lowerType.Contains("pttypfloat64") || lowerType.Contains("pttypdouble"))
            {
                return TSDataType.DOUBLE;
            }
            else if (lowerType.Contains("pttypfloat16") || lowerType.Contains("pttypfloat32"))
            {
                return TSDataType.FLOAT;
            }
            else
            {
                return TSDataType.FLOAT;
            }
        }

        static TSEncoding GetEncodingByType(TSDataType dataType)
        {
            if (dataType == TSDataType.BOOLEAN)
            {
                return TSEncoding.RLE;
            }
            else if (dataType == TSDataType.INT32 || dataType == TSDataType.INT64 || dataType == TSDataType.TIMESTAMP)
            {
                return TSEncoding.TS_2DIFF;
            }
            else if (dataType == TSDataType.FLOAT || dataType == TSDataType.DOUBLE)
            {
                return TSEncoding.GORILLA;
            }
            else if (dataType == TSDataType.TEXT || dataType == TSDataType.STRING || dataType == TSDataType.BLOB)
            {
                return TSEncoding.PLAIN;
            }
            else
            {
                return TSEncoding.RLE;
            }
        }

        class PiPointInfo
        {
            public string PointName { get; set; }
            public string PiDataType { get; set; }
            public string Description { get; set; }
        }
    }
}
