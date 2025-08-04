# ChromaSky™ Backend

一个用于自动化预测和可视化火烧云天象的后端服务与计算引擎。

本项目通过获取最新的全球气象预报数据（GFS）和大气成分数据（CAMS），结合 ChromaSky™ 火烧云指数算法，计算出未来特定时间（日出/日落）的火烧云观赏指数，并生成专业、美观的栅格预报图。

## ✨ 核心特性

- **自动化数据流**: 定时任务自动从 NOAA 和 Copernicus 拉取最新的 GFS 和 CAMS 预报数据。
- **高级科学计算**: 基于 `xarray`, `pandas`, `shapely` 等库进行高效、精准的地理空间数据处理。
- **ChromaSky™ 核心算法**: 独创的多因子评分模型，综合评估影响火烧云形成的各项关键气象条件。
- **批处理与综合预报**: 对多个时间点进行批量计算，并将结果叠加生成更鲁棒的综合指数，提高预报的命中率。
- **专业级地图可视化**: 使用 `matplotlib` 和 `cartopy` 生成带有等值线和平滑渲染的高质量栅格预报图，效果媲美专业气象产品。
- **灵活的命令行工具**: 提供方便的命令行接口，可按需生成指定事件（今日/明日的日出/日落）的预报图。
- **API 服务框架**: 基于 FastAPI 构建，为未来提供在线查询、图片服务等功能预留了扩展接口。

## 🔭 ChromaSky™ 火烧云指数计算核心算法介绍

ChromaSky™ 指数是一个范围在 0-10 之间的综合评分，旨在量化在特定地点和时间观赏到壮丽火烧云（朝霞或晚霞）的可能性和质量。分数越高，代表条件越理想。

该算法的核心思想和多因子模型，主要受 [Sunset Bot 知识库](https://sunsetbot.top/knowledge/) 中关于天象预测的宝贵资料启发。我们在此基础上，结合实际可获取的 GFS 和 CAMS 预报数据，设计并实现了具体的评分函数。我们对原作者的分享表示诚挚的感谢。

**最终计算公式:**
`Final Score = (Factor A × Factor B × Factor C × Factor D) × 10`

---

### Factor A: 天空画布质量 (Local Clouds Score)

**理念**: 火烧云需要云，但不是任何云都可以。我们需要漂浮在中高空的云层作为画布，来反射地平线下太阳的最后一缕光辉。低云会遮挡视线，而晴空万里则没有“画布”。

**评分逻辑 (`score_local_clouds`)**:
此因子评估的是**中云**和**高云**的总覆盖率（`canvas_cover = medium_cloud + high_cloud`）。
- **最佳状态 (得分: 1.0)**: 当中高云总覆盖率达到或超过 **20%** 时，我们认为天空拥有了绝佳的画布。
- **增长阶段 (得分: 0.1 ~ 1.0)**: 当中高云总覆盖率在 **0% 至 20%** 之间时，分数会从一个基础分 `0.1` 开始，随云量**线性增长**。云量越接近20%，得分越高。这意味着少量的高云也能产生一定的观赏效果，但不如大面积的云层壮观。

---

### Factor B: 光路纯净度 (Light Path Score)

**理念**: 在日落时，太阳光需要从遥远的地平线方向穿过漫长的大气才能照射到观测地上空的云层。如果这条光路上布满了低云，光线就会被中途阻挡，无法点亮“画布”。

**评分逻辑 (`score_light_path`)**:
此因子使用**本地总云量 (`tcc`)** 作为远方光路清洁度的近似指标。这是一个简化但有效的模型。
- **逻辑**: `Score = ((100 - tcc) / 100) ** 2`
- **效果**: 这是一个二次递减函数。总云量越低，得分越高。当总云量从0%增加时，得分下降缓慢；而当总云量很高时，得分会急剧下降。这很好地模拟了少量低云影响不大，但大量低云会造成毁灭性遮挡的现实情况。

---

### Factor C: 空气质量 (Air Quality Score) - 可选

**理念**: 空气中的悬浮颗粒（气溶胶）对光线的散射至关重要。极度纯净的空气会让色彩较为平淡，而适当的气溶胶能散射出更丰富、更温暖的红橙色调。但过量的气溶胶（如雾霾）则会吸收和阻挡光线，使天空变得灰暗。

**评分逻辑 (`score_air_quality`)**:
此因子基于 **AOD (550nm气溶胶光学厚度)** 进行评分。
- **最佳状态 (得分: 1.0)**: 当 AOD 低于 `0.2` 时，代表空气非常通透，光线损失最小。
- **线性递减 (得分: 1.0 ~ 0.0)**: 当 AOD 在 `0.2` 到 `0.8` 之间时，分数线性下降。
- **差状态 (得分: 0.0)**: 当 AOD 高于 `0.8` 时，代表空气污染严重或有沙尘，光线难以穿透，得分为0。

---

### Factor D: 云层高度 (Cloud Altitude Score)

**理念**: 云层越高，它能被地平线下的太阳照亮的时间就越长，色彩也越梦幻。高空卷云（>6000米）是制造史诗级火烧云的最佳选择。

**评分逻辑 (`score_cloud_altitude`)**:
这是一个基于**云底高 (`cloud_base_height`)** 的分段函数。
- **绝佳 (得分: 1.0)**: 云底高 > 6000米 (高云族)。
- **良好 (得分: 0.7)**: 云底高在 2500米 到 6000米之间 (中云族)。
- **一般 (得分: 0.3)**: 云底高 < 2500米 (低云)。

---

### 综合预报逻辑 (Composite Logic)

为了提高预报的鲁棒性，系统会对一个时间段内的多个时刻（例如，日落的 17:00, 18:00, 19:00）分别计算完整的得分矩阵。最终，在每一个地理格点上，取所有时刻中的**最高分**作为该点的最终综合得分。这确保了只要在整个日落时段内有一次绝佳机会，就会被预报出来。

## ⚙️ 系统工作流

1.  **调度执行**: 运行 `scheduler.py` 启动主调度任务。
2.  **数据下载**: 调度器会依次触发 `cams_tasks.py` 和 `gfs_tasks.py`，根据当前时间和配置 (`download_config.py`)，下载未来所需的 CAMS AOD 和 GFS 气象数据，并保存到 `grib_data/` 目录。
3.  **地图生成**: （当前为手动步骤）运行 `draw_score_map.py` 脚本。
4.  **计算与叠加**: 脚本会为目标事件（如 `today_sunset`）的所有批处理时间点，加载数据并计算得分矩阵。然后将这些矩阵叠加（取最大值）生成一个综合得分场。
5.  **区域裁剪与平滑**: 使用天文算法计算出的事件可见范围对得分场进行裁剪，并应用高斯平滑和插值算法提升视觉效果。
6.  **图像绘制**: 使用 `matplotlib` 和 `cartopy` 将最终的得分场绘制成带有等值线和地理信息的PNG图片，保存到 `map_images/` 目录。

## 🚀 安装与配置

**1. 环境准备**
- Python 3.12+
- `uv` 包管理器 (推荐, `pip install uv`)
- Cartopy 依赖的系统库 (如 GEOS, PROJ)。推荐使用 `conda` 或系统包管理器（如 `sudo apt-get install libproj-dev proj-data proj-bin libgeos-dev`）进行安装。

**2. 克隆与安装**
```bash
git clone https://github.com/your-username/chromasky-backend.git
cd chromasky-backend

# 创建并激活虚拟环境
uv venv

# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

# 安装所有依赖
uv pip sync
```

**3. 配置 Copernicus API Key**
- 访问 [Copernicus ADS](https://ads.climate.copernicus.eu/#!/home) 注册账户。
- 在个人主页找到你的 API Key。
- 在你的用户主目录下创建 `.cdsapirc` 文件，并填入以下内容：
  ```
  url: https://ads.climate.copernicus.eu/api/v2
  key: YOUR_UID:YOUR_API_KEY
  ```

## 🏃 如何使用

**1. 运行调度器下载数据**
```bash
python scheduler.py
```
*该步骤会下载最新的 GFS 和 CAMS 数据，请确保已正确配置 `.cdsapirc`。*

**2. 生成预报图**
脚本 `draw_score_map.py` 支持命令行参数。

- **生成默认的“今日日落”图**:
  ```bash
  python draw_score_map.py
  ```

- **生成指定的“明日日出”图**:
  ```bash
  python draw_score_map.py tomorrow_sunrise
  ```

- **生成预报图并禁用AOD因子**:
  ```bash
  python draw_score_map.py today_sunset --no-aod
  ```

- **查看所有可用选项**:
  ```bash
  python draw_score_map.py --help
  ```

生成的图片会保存在项目根目录下的 `map_images/` 文件夹中。

## 📝 未来计划

- [ ] 将地图生成任务 (`draw_score_map.py`) 集成到 `scheduler.py` 的自动化流程中。
- [ ] 完善 FastAPI 服务，提供一个API端点用于获取最新的预报图URL或图片本身。
- [ ] 更新 `frontend/index.html`，使其能够加载并叠加显示由后端生成的栅格图像图层。
- [ ] 进一步研究和调优 ChromaSky™ 指数的评分模型。

## 📜 许可证

本项目采用 [MIT License](LICENSE) 授权。