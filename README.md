# PI-To-IoTDB-Tools

PI 数据迁移工具 - 将 OSIsoft PI 数据库中的点位和历史数据迁移到 Apache IoTDB。

## 功能特性

- **点位迁移**: 从 PI 读取点位元数据（名称、类型、描述），批量在 IoTDB 创建时序
- **数据迁移**: 流式读取 PI 历史数据，边读边写至 IoTDB，内存友好
- **断点续传**: 支持从指定点位开始迁移（补数场景）
- **批量写入**: 单点位数据支持分批次插入（每批10万条）
- **错误重试**: 写入失败自动重试机制

## 技术栈

- .NET Framework 4.7.2
- Apache IoTDB 2.0+
- OSIsoft PI SDK (COM Interop)

## 执行流程

```
┌─────────────────────────────────────────────────────────────┐
│  阶段1：PI点位迁移                                         │
│  • 从PI读取点位元数据                                      │
│  • 批量创建IoTDB时序                                       │
│  • 设置时序描述属性                                        │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  阶段2：历史数据迁移                                       │
│  • 流式读取点位历史数据                                    │
│  • 边读边写至IoTDB                                        │
└─────────────────────────────────────────────────────────────┘
```

## 配置说明

### 连接配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| piServerAddress | RTDB | PI 服务器地址 |
| iotdbHost | 127.0.0.1 | IoTDB 主机地址 |
| iotdbPort | 6667 | IoTDB 端口 |
| iotdbUser | root | IoTDB 用户名 |
| iotdbPwd | TimechoDB@2021 | IoTDB 密码 |
| iotdbRootDevice | root.pi.test | 根设备路径 |

### 数据范围配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| dataStartTime | 2025-10-25 00:00:00 | 数据开始时间 |
| dataEndTime | 2025-10-30 00:00:00 | 数据结束时间 |

### 性能配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| parallelTaskCount | CPU核心数 × 2 | 并行任务数 |
| maxRetry | 2 | 失败重试次数 |
| BatchSize | 100000 | 每批插入条数 |

## 使用方法

### 1. 安装依赖

确保已安装：
- OSIsoft PI SDK
- Apache IoTDB 2.0+

### 2. 编译项目

```bash
msbuild import-pi-data.csproj /t:Build /p:Configuration=Debug
```

### 3. 运行

```bash
cd bin/Debug
import-pi-data.exe
```

### 4. 补数场景

如需从指定点位开始迁移（断点续传），修改 `TargetStartPointName` 配置：

```csharp
private static readonly string TargetStartPointName = "YOUR_START_POINT_NAME";
```

## 文件结构

```
PI-To-IoTDB-Tools/
├── import-pi-data/
│   ├── Program.cs          # 主程序入口，数据迁移逻辑
│   ├── PiPointMigration.cs # 点位迁移逻辑
│   ├── import-pi-data.csproj
│   └── appsettings.json    # 配置文件
├── .gitignore
└── README.md
```

## 代码说明

### 主程序入口

`Program.cs` 中的 `Main()` 方法控制整体流程：

1. 调用 `PiPointMigration.RunMigrationAsync()` 执行点位迁移
2. 读取点位元数据到队列
3. 调用 `StreamProcess()` 执行流式数据迁移

### 点位迁移

`PiPointMigration.RunMigrationAsync()`:
- 从 PI 分批读取点位元数据
- 调用 `CreateMultiTimeSeriesAsync` 批量创建时序
- 为每个时序设置描述属性

### 数据迁移

`StreamProcess()`:
- 使用信号量控制并发数
- 每个点位独立任务处理
- 读取→写入→释放内存，循环处理

## 注意事项

1. 确保 PI 服务器可访问，且 PI SDK 已正确安装
2. IoTDB 需提前启动，网络连通正常
3. 首次运行建议先进行点位迁移，确认时序创建成功后再迁移数据
4. 大数据量迁移建议分批次进行，避免内存溢出

## 许可证

MIT License
