CREATE SCHEMA IF NOT EXISTS NSK;

CREATE TABLE IF NOT EXISTS NSK.hourly_agg_view(
    "time"	varchar(30)	NOT NULL PRIMARY KEY,
    avg_temperature_2m_24h	numeric,
    avg_relative_humidity_2m_24h	numeric,
    avg_dew_point_2m_24h	numeric,
    avg_apparent_temperature_24h	numeric,
    avg_temperature_80m_24h	numeric,
    avg_temperature_120m_24h	numeric,
    avg_wind_speed_10m_24h	numeric,
    avg_wind_speed_80m_24h	numeric,
    avg_visibility_24h	numeric,
    total_rain_24h	numeric,
    total_showers_24h	numeric,
    total_snowfall_24h	numeric,
    avg_temperature_2m_daylight	numeric,
    avg_relative_humidity_2m_daylight	numeric,
    avg_dew_point_2m_daylight	numeric,
    avg_apparent_temperature_daylight	numeric,
    avg_temperature_80m_daylight	numeric,
    avg_temperature_120m_daylight	numeric,
    avg_wind_speed_10m_daylight	numeric,
    avg_wind_speed_80m_daylight	numeric,
    avg_visibility_daylight	numeric,
    total_rain_daylight	numeric,
    total_showers_daylight	numeric,
    total_snowfall_daylight	numeric,
);

CREATE TABLE IF NOT EXISTS NSK.hourly(
    time_iso varchar(30) NOT NULL PRIMARY KEY,
    wind_speed_10m_m_per_s	numeric,
    wind_speed_80m_m_per_s	numeric,
    temperature_2m_celsius	numeric,
    apparent_temperature_celsius	numeric,
    temperature_80m_celsius	numeric,
    temperature_120m_celsius	numeric,
    soil_temperature_0cm_celsius	numeric,
    soil_temperature_6cm_celsius	numeric,
    rain_mm	numeric,
    showers_mm	numeric,
    snowfall_mm numeric
);

CREATE TABLE IF NOT EXISTS NSK.daily(
    time_iso varchar(30) NOT NULL PRIMARY KEY,
    sunrise_iso varchar(30),
    sunset_iso varchar(30),
    daylight_hours smallint
)