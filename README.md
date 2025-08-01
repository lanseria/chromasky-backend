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