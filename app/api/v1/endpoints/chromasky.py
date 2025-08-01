# app/api/v1/endpoints/chromasky.py
from fastapi import APIRouter, HTTPException, Query
from app.services.data_fetcher import data_fetcher, EventType
from app.services import chromasky_calculator as calculator

router = APIRouter()
@router.get("/data_check")
def check_data_for_point(
    event: EventType = Query(
        default="today_sunset",
        description="选择要查询的预报事件: 'today_sunrise', 'today_sunset', 'tomorrow_sunrise', 'tomorrow_sunset'"
    ),
    lat: float = Query(
        default=29.800,
        description="纬度 (Latitude)",
        ge=-90,
        le=90
    ),
    lon: float = Query(
        default=121.740,
        description="经度 (Longitude)",
        ge=-180,
        le=360
    )
):
    """
    一个用于调试的端点，返回原始数据以及计算出的因子得分。
    """
    # 1. 获取本地的原始 GFS 数据
    raw_gfs_data = data_fetcher.get_all_variables_for_point(lat=lat, lon=lon, event=event)
    if "error" in raw_gfs_data:
        raise HTTPException(status_code=404, detail=raw_gfs_data["error"])

    # 2. 获取 AOD 数据
    aod_value = data_fetcher.get_aod_for_event(lat=lat, lon=lon, event=event)
        
    # 3. 计算光路上的平均云量 (因子B的输入)
    avg_cloud_path = data_fetcher.get_light_path_avg_cloudiness(lat=lat, lon=lon, event=event)

    # 4. 计算各个因子的得分
    factor_a_score = calculator.score_local_clouds(
        raw_gfs_data.get("high_cloud_cover"), raw_gfs_data.get("medium_cloud_cover")
    )
    factor_b_score = calculator.score_light_path(avg_cloud_path)
    factor_c_score = calculator.score_air_quality(aod_value)
    factor_d_score = calculator.score_cloud_altitude(raw_gfs_data.get("cloud_base_height_meters"))
    
    # 从 data_fetcher 实例中获取对应事件的 GFS 时间元数据
    gfs_time_info = data_fetcher.gfs_time_metadata.get(event)
    aod_time_info = data_fetcher.aod_time_metadata # AOD 时间元数据是全局的
    
    return {
        "message": f"成功获取事件 '{event}' 的原始数据及因子得分",
        "time_info": {
            "gfs_forecast": gfs_time_info,
            "aod_forecast": aod_time_info
        },
        "location": {"lat": lat, "lon": lon},
        "raw_data": {
            **raw_gfs_data, # 合并字典
            "aod": round(aod_value, 3) if aod_value is not None else None
        },
        "calculated_factors": {
            "factor_A_local_clouds": {
                "score": round(factor_a_score, 2),
                "input_hcc": raw_gfs_data.get("high_cloud_cover"),
                "input_mcc": raw_gfs_data.get("medium_cloud_cover"),
            },
            "factor_B_light_path": {
                "score": round(factor_b_score, 2),
                "input_avg_tcc_along_path": round(avg_cloud_path, 2) if avg_cloud_path is not None else None,
            },
            "factor_C_air_quality": {
                "score": round(factor_c_score, 2),
                "input_aod": round(aod_value, 3) if aod_value is not None else None,
            },
            "factor_D_cloud_altitude": {
                "score": round(factor_d_score, 2),
                "input_cloud_base_meters": raw_gfs_data.get("cloud_base_height_meters"),
            }
        }
    }

@router.get("/")
def get_chromasky_index(
    event: EventType = Query(
        default="today_sunset",
        description="选择要查询的预报事件"
    ),
    lat: float = Query(
        default=31.23,
        description="纬度 (Latitude)",
        ge=-90,
        le=90
    ),
    lon: float = Query(
        default=121.47,
        description="经度 (Longitude)",
        ge=-180,
        le=360
    )
):
    """
    获取指定经纬度和事件的ChromaSky指数和详细分项得分。
    """
    # 1. 获取所有必需的原始数据
    raw_gfs_data = data_fetcher.get_all_variables_for_point(lat=lat, lon=lon, event=event)
    if "error" in raw_gfs_data:
        raise HTTPException(status_code=404, detail=raw_gfs_data["error"])

    aod_value = data_fetcher.get_aod_for_event(lat=lat, lon=lon, event=event)
    avg_cloud_path = data_fetcher.get_light_path_avg_cloudiness(lat=lat, lon=lon, event=event)
    
    # 2. 调用计算器服务来计算最终得分和分项
    calculation_result = calculator.calculate_final_score(
        raw_gfs_data=raw_gfs_data,
        aod_value=aod_value,
        avg_cloud_path=avg_cloud_path
    )
    
    # 3. 组合最终的 API 响应
    gfs_time_info = data_fetcher.gfs_time_metadata.get(event)
    
    return {
        "location": {"lat": lat, "lon": lon},
        "event": event,
        "time_info": {"gfs_forecast": gfs_time_info},
        **calculation_result # 使用字典解包合并得分和分项
    }
