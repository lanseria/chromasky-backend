### 项目目标

该项目旨在创建一个“火烧云指数”预报服务。它通过从气象数据源（NOAA GFS 和 Copernicus CAMS）获取预报数据，经过一系列计算，最终生成一个量化的指数。项目通过两种方式提供服务：

1.  **API 端点**：提供查询特定地理坐标、特定事件（如今日日落）的火烧云指数。
2.  **静态 GeoJSON 文件**：为前端地图生成预先计算好的格点数据，展示大范围内的指数分布。

### 核心工作流程

整个系统的工作流程可以分为三个主要阶段：**数据采集**、**数据处理**和**数据服务**。

1.  **数据采集 (由 `scheduler.py` 驱动)**
    *   **调度入口**：`scheduler.py` 是整个自动化流程的起点，它按顺序执行 CAMS 和 GFS 的数据下载任务。
    *   **CAMS AOD 数据**：`app/tasks/cams_tasks.py` 负责使用 `cdsapi` 从哥白尼服务下载气溶胶光学厚度（AOD）数据。它会智能判断应下载前一天还是当天的预报，并将数据和清单文件（`manifest_aod.json`）保存在 `grib_data/cams_aod/` 目录下。
    *   **GFS 云图数据**：`app/tasks/gfs_tasks.py` 负责下载核心的 GFS 气象数据。它会自动判断当前可用的最新 GFS 运行周期（例如 `00z`, `06z` 等），然后计算未来几个关键事件（今日/明日的日出日落）的预报时间点。接着，它调用 `app/services/grib_downloader.py` 下载不同层级和类型的云量、云底高度等 GRIB 文件，并按事件组织文件结构，最后生成一个运行周期的总清单（如 `manifest_20250802_00.json`）。

2.  **数据处理 (由 `app/tasks/processing_tasks.py` 负责)**
    *   **触发机制**：GFS 数据下载成功后，会立即触发 `run_geojson_generation_task` 任务。
    *   **数据加载**：该任务首先会强制 `DataFetcher` 单例重新加载数据，以确保它能读取到刚刚下载的最新 GRIB 文件。
    *   **计算与生成**：它使用 `ChromaSkyCalculator`，为每个事件（如 `today_sunset`）的所有格点并行计算火烧云指数，并将结果生成为高密度的 GeoJSON 文件。
    *   **清单更新**：生成好的 GeoJSON 文件被保存在 `frontend/gfs/{run_id}/` 目录下。同时，它会更新一个位于 `frontend/gfs/` 的主清单文件 `gfs_data_manifest.json`，将 `latest_run` 指向刚刚完成的这批数据，方便前端应用直接加载。

3.  **数据服务 (由 `app/main.py` 和 FastAPI 提供)**
    *   **启动预加载**：当 FastAPI 应用启动时，`lifespan` 管理器会初始化 `DataFetcher` 单例，它会根据最新的清单文件将 GRIB 数据加载到内存中，以 `xarray.Dataset` 的形式缓存，从而实现快速查询。
    *   **API 接口**：`app/api/v1/endpoints/chromasky.py` 中定义了几个核心 API：
        *   `/`: 查询单个点的实时指数，它会利用内存中的数据进行快速计算。
        *   `/map_data`: 动态生成指定密度的地图数据（作为静态文件的补充或动态查询方式）。
        *   `/data_check`: 一个非常实用的调试接口，用于查看某个点的原始气象数据和计算因子。
    *   **静态文件服务**：FastAPI 同时将 `frontend` 目录作为静态文件目录，使得前端可以直接请求 `gfs_data_manifest.json` 以及其指向的各个 GeoJSON 文件。

### 关键模块和技术栈

*   **FastAPI & Uvicorn**: 提供了高性能的异步 Web 服务框架。
*   **xarray & cfgrib**: 项目的核心，用于读取和操作 GRIB 格式的气象数据。我注意到您在 `data_fetcher.py` 中使用了 `backend_kwargs={'filter_by_keys': {'stepType': 'instant'}}` 来解决 GRIB 文件中可能存在的键冲突问题，这是处理复杂 GRIB 数据的正确实践。
*   **Requests & cdsapi**: 用于从外部 HTTP 服务（NOAA NOMADS）和 API（Copernicus）下载数据。
*   **`DataFetcher` (单例模式)**: 设计巧妙的数据缓存中心，通过在应用启动时预加载，极大地提升了 API 响应速度。
*   **`ChromaSkyCalculator`**: 封装了项目的核心算法和业务逻辑，将原始气象数据转化为最终的“火烧云指数”。
*   **`scheduler.py` & `app/tasks/*`**: 构成了一个健壮的后台任务调度和执行系统，实现了数据获取和处理的自动化。
*   **`debug_grib.py`**: 一个独立的调试工具，表明您在开发过程中对 GRIB 文件复杂性的深入研究。


### ChromaSky™ 火烧云指数计算核心算法

本项目的核心是 `ChromaSky™ 指数`，这是一个综合评分（0-10分），用于量化在特定地点和时间观赏到壮观日出/日落（即“火烧云”）的潜力。该指数的计算基于一个四因子乘法模型，任何一个因子的缺失都会显著降低总分。

#### 最终指数公式

```
指数 = 因子A × 因子B × 因子C × 因子D × 10
```

其中，每个因子的取值范围均为 `0.0` 到 `1.0`。

---

#### 因子A: 本地云况 (The Canvas)

**目的**: 评估观测点上空是否存在适合被霞光染色的“画布”（即中高云）。

**公式**:
```python
# hcc = 高云量 (%), mcc = 中云量 (%)
canvas_cloud_cover = hcc + mcc

if canvas_cloud_cover < 20:
    factor_A = 0.1  # 云量太少，得分极低
else:
    factor_A = 1.0  # 云量充足，得满分
```
*   **数据源**: `hcc` (高云量), `mcc` (中云量)
*   **解读**: 没有中高云作为画布，即使光线再好也无法形成壮观的火烧云。

---

#### 因子B: 光照路径 (The Window)

**目的**: 评估从太阳到观测点上空云层的光路是否干净，即日出/日落方向是否存在清晰的“晴空窗口”。这是最重要的影响因子。

**公式**:
```python
# avg_tcc_along_path = 光路上的平均总云量 (%)
clarity = (100 - avg_tcc_along_path) / 100

factor_B = clarity ** 2
```
*   **数据源**: `tcc` (总云量)
*   **解读**:
    *   需要沿着日出/日落的方位角，从观测点向外回溯约 400-500 公里，采样多个点的总云量并计算其平均值 `avg_tcc_along_path`。
    *   使用**平方** (`** 2`) 是为了加大对路径中有云的惩罚。即使路径上只有 30% 的云量，`clarity` 为 `0.7`，最终得分 `factor_B` 也会降至 `0.49`，对总分产生巨大影响。

---

#### 因子C: 空气质量 (The Filter)

**目的**: 评估光路上的大气通透度。干净的空气能有效散射短波光（蓝光），让长波光（红光）通过，形成鲜艳的色彩。浑浊的空气则会直接吸收和散射所有光线，使天空变得灰暗。

**公式 (基于AOD估算)**:
```python
# aod = 气溶胶光学厚度 (Aerosol Optical Depth)
if aod < 0.2:
    factor_C = 1.0  # 空气极佳
elif aod > 0.8:
    factor_C = 0.0  # 空气极差
else:
    # 在 0.2 到 0.8 之间线性下降
    factor_C = 1.0 - ((aod - 0.2) / 0.6)
```
*   **数据源**:
    *   **最佳**: ECMWF CAMS 或 GEFS-Aerosols 模型的 AOD 数据。
    *   **当前项目 (V1)**: 暂时使用一个基于季节的**估算值**，例如夏季 `0.4`，冬季 `0.2`。

---

#### 因子D: 云层高度 (The Scale)

**目的**: 评估云层的高度。越高的云能被阳光照射的时间越长，形成的火烧云规模也可能更宏大。

**公式**:
```python
# cloud_base_meters = 云底高度 (米)
if cloud_base_meters is None or isnan(cloud_base_meters):
    factor_D = 0.0 # 没有云
elif cloud_base_meters > 6000:
    factor_D = 1.0 # 高云，最佳
elif cloud_base_meters > 2500:
    factor_D = 0.7 # 中云，良好
else:
    factor_D = 0.3 # 低云，较差
```
*   **数据源**: `gh` (位势高度) @ `cloudCeiling` (云幂) 层级，直接获取最低云层的底部高度。

---

### **实现流程概览**

1.  **数据准备**: 后台定时任务 (`scheduler.py`) 每日两次从 GFS 下载未来四个关键事件（今明两天日出日落）的 `tcc`, `hcc`, `mcc`, `lcc`, `gh` 等数据，并生成 `manifest.json`。
2.  **API 请求**: 用户通过 API 请求特定地点 (`lat`, `lon`) 和事件 (`event`) 的指数。
3.  **数据提取**: `data_fetcher.py` 从缓存中提取该事件、该地点的原始气象数据。
4.  **光路分析**: `chromasky_calculator.py` 计算太阳方位角，并沿着该路径回溯采样，计算出 `avg_tcc_along_path`。
5.  **指数计算**: `chromasky_calculator.py` 调用上述四个评分函数，计算出各因子得分和最终总分。
6.  **返回结果**: API 将总分和各因子分项得分一并返回给前端。