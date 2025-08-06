#!/usr/bin/env python
import datetime
from ecmwfapi import ECMWFDataServer

# ------------------- 配置参数 -------------------

# 1. 定义地理范围 (北, 西, 南, 东)
#    注意：API 使用 (North, West, South, East) 顺序
CDS_AREA_EXTRACTION = {
    "north": 54.00,
    "south": 0.00,
    "west": 70.00,
    "east": 135.00,
}
# 将字典转换为 API 需要的 "N/W/S/E" 格式字符串
area_string = f"{CDS_AREA_EXTRACTION['north']}/{CDS_AREA_EXTRACTION['west']}/{CDS_AREA_EXTRACTION['south']}/{CDS_AREA_EXTRACTION['east']}"

# 2. 定义请求的参数
#    hcc (188), mcc (187), lcc (186), cbh (159)
param_string = "188.128/187.128/186.128/159.128"

# 3. 定义预报步长
#    您需要 11, 12, 13, 14 小时
step_string = "11/12/13/14"

# 4. 自动确定最新的预报日期和时间
#    ECMWF 每天有 00Z 和 12Z 两次预报。
#    此逻辑会选择离当前时间最近的一次已发布的预报。
now = datetime.datetime.utcnow()
if now.hour < 6: # 00Z-05Z, 12Z 的前一天数据最新
    run_date = now - datetime.timedelta(days=1)
    run_time = "12:00:00"
elif 6 <= now.hour < 18: # 06Z-17Z, 当天 00Z 的数据最新
    run_date = now
    run_time = "00:00:00"
else: # 18Z-23Z, 当天 12Z 的数据最新
    run_date = now
    run_time = "12:00:00"

# 格式化日期为 YYYYMMDD
date_string = run_date.strftime("%Y%m%d")

# 5. 定义输出文件名
output_filename = f"ecmwf_cloud_forecast_{date_string}_{run_time[:2]}Z.nc"


# ------------------- 执行数据请求 -------------------

print("--- 开始数据下载任务 ---")
print(f"请求的预报: {date_string} at {run_time}")
print(f"预报步长 (小时): {step_string}")
print(f"地理范围 (N/W/S/E): {area_string}")
print(f"输出文件: {output_filename}")
print("---------------------------\n")

# 初始化 ECMWF 数据服务器客户端
server = ECMWFDataServer()

# 发送 MARS 请求
try:
    server.retrieve({
        "class": "od",              # Operational Data
        "stream": "oper",           # Operational forecast stream
        "type": "fc",               # Forecast
        "levtype": "sfc",           # Surface level parameters
        
        "date": date_string,        # 自动计算的最新预报日期
        "time": run_time,           # 自动计算的最新预报时间 (00Z 或 12Z)
        
        "param": param_string,      # 请求的云参数
        "step": step_string,        # 请求的预报步长
        
        "grid": "0.25/0.25",        # 空间分辨率
        "area": area_string,        # 地理范围
        
        "format": "netcdf",         # 输出格式为 NetCDF
        "target": output_filename,  # 输出文件名
    })
    print(f"\n✅ 数据下载成功！文件已保存为: {output_filename}")

except Exception as e:
    print(f"\n❌ 数据下载失败！")
    print(f"错误信息: {e}")
    print("请检查：")
    print("1. 您的网络连接是否正常。")
    print("2. 您的 ECMWF 账户是否有权限访问此数据。")
    print("3. .ecmwfapirc 配置文件是否正确。")