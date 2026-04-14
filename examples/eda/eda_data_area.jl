# NOTE:
#   data["area"] is not from .csv and .arrow, but from
# SiennaNEM.read_data.add_area_df!

using SiennaNEM

bus_to_area = SiennaNEM.get_map_from_df(data["bus"], :id_bus, :id_area)
SiennaNEM.add_id_area_col!(data["generator"], bus_to_area)
SiennaNEM.add_area_df!(data)

"""
    add_area_data_col!(
    df, area_df;
    id_area_col=:id_area,
    area_name_col=:area_name,
    area_df_id=:id_area,
    area_df_name=:name,
)

Add an `area_name` column to `df` by matching `df[id_area_col]` with `area_df[area_df_id]`
and using `area_df[area_df_name]` as the label.

- Requires `df` to already have `id_area_col`.
- Throws if an `id_area` in `df` is not present in `area_df`.
"""
function add_area_data_col!(
    df, map;
    id_area_col::Symbol=:id_area,
    data_col::Symbol=:area_name,
)
    # NOTE: area_to_name is a constant from SiennaNEM.const
    df[!, data_col] = [map[id] for id in df[!, id_area_col]]
end

add_area_data_col!(data["bus"], SiennaNEM.area_to_name; data_col=:area_name)
add_area_data_col!(data["bus"], SiennaNEM.area_to_tref_summer; data_col=:tref_summer)
add_area_data_col!(data["bus"], SiennaNEM.area_to_tref_winter; data_col=:tref_winter)

data["bus"]
# 12×10 DataFrame
#  Row │ id_bus  name    alias                           active  latitude  longitude  id_area  area_name  tref_summer  tref_winter 
#      │ Int64   String  String                          Bool    Float64   Float64    Int64    String     Float64      Float64     
# ─────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │      1  NQ      Northern Queensland               true  -17.7938    145.564        1  QLD               32.0         15.0
#    2 │      2  CQ      Central Queensland                true  -22.8242    149.404        1  QLD               32.0         15.0
#    3 │      3  GG      Gladstone Grid                    true  -23.8429    151.249        1  QLD               32.0         15.0
#    4 │      4  SQ      Southern Queensland               true  -27.4766    153.03         1  QLD               32.0         15.0
#    5 │      5  NNSW    Northern New South Wales          true  -30.5047    151.652        2  NSW               32.0          9.0
#    6 │      6  CNSW    Central New South Wales           true  -33.4833    150.158        2  NSW               32.0          9.0
#    7 │      7  SNW     Sydney, Newcastle & Wollongong    true  -33.865     151.209        2  NSW               32.0          9.0
#    8 │      8  SNSW    Southern New South Wales          true  -35.111     147.36         2  NSW               32.0          9.0
#    9 │      9  VIC     Victoria                          true  -37.7661    144.943        3  VIC               32.0          8.0
#   10 │     10  TAS     Tasmania                          true  -42.8806    147.325        4  TAS               35.0         11.0
#   11 │     11  CSA     Central South Australia           true  -34.8027    138.522        5  SA                 7.7          1.2
#   12 │     12  SESA    South East South Australia        true  -37.6047    140.837        5  SA                 7.7          1.2

add_area_data_col!(data["area"], SiennaNEM.area_to_tref_summer; data_col=:tref_summer)
add_area_data_col!(data["area"], SiennaNEM.area_to_tref_winter; data_col=:tref_winter)
data["area"]
# 5×7 DataFrame
#  Row │ id_area  name    peak_active_power  peak_reactive_power  max_pmax  tref_summer  tref_winter 
#      │ Int64    String  Float64            Float64              Float64   Float64      Float64     
# ─────┼─────────────────────────────────────────────────────────────────────────────────────────────
#    1 │       1  QLD                   0.0                  0.0   2042.66         32.0         15.0
#    2 │       2  NSW                   0.0                  0.0   2345.46         32.0          9.0
#    3 │       3  VIC                   0.0                  0.0   5362.16         32.0          8.0
#    4 │       4  TAS                   0.0                  0.0    563.35         35.0         11.0
#    5 │       5  SA                    0.0                  0.0   2435.99          7.7          1.2

SiennaNEM.add_id_area_col!(data["line"], bus_to_area; bus_col=:id_bus_from, area_col=:id_area_from)
SiennaNEM.add_id_area_col!(data["line"], bus_to_area; bus_col=:id_bus_to, area_col=:id_area_to)
add_area_data_col!(data["line"], SiennaNEM.area_to_name; id_area_col=:id_area_from, data_col=:area_from)
add_area_data_col!(data["line"], SiennaNEM.area_to_name; id_area_col=:id_area_to,   data_col=:area_to)

transform!(
    data["line"],
    [:tmax, :tmin] =>
        ByRow((tmax, tmin) -> (tmax <= tmin ? :tmax : :tmin)) =>
        :dir_limit,
)
transform!(
    data["line"],
    [:dir_limit, :id_bus_from, :id_bus_to] =>
        ByRow((dir, id_from, id_to) -> (dir === :tmax ? id_from : id_to)) =>
        :id_bus_limit,
)
SiennaNEM.add_id_area_col!(data["line"], bus_to_area; bus_col=:id_bus_limit, area_col=:id_area_limit)

transform!(
    data["line"],
    :id_lin => ByRow(id -> get(line_to_tmin_summer, id, NaN)) => :tmin_summer,
)
transform!(
    data["line"],
    :id_lin => ByRow(id -> get(line_to_tmax_summer, id, NaN)) => :tmax_summer,
)

show(filter(:investment => ==(false), filter(:active => ==(true), data["line"]))[:, [:id_lin, :alias, :area_from, :area_to, :id_bus_from, :id_bus_to, :tmax, :tmin, :dir_limit, :id_bus_limit, :tmax_summer, :tmin_summer]],  allrows=true, allcols=true)
# 15×8 DataFrame
#  Row │ id_lin  alias                  id_bus_from  id_bus_to  tmax     tmin     dir_limit  id_bus_limit 
#      │ Int64   String                 Int64        Int64      Float64  Float64  Symbol     Int64        
# ─────┼──────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │      1  CQ->NQ                           2          1   1400.0   1400.0  tmax                  2
#    2 │      2  CQ->GG                           2          3   1050.0   1100.0  tmax                  2
#    3 │      3  SQ->CQ                           4          2   1100.0   2100.0  tmax                  4
#    4 │      4  QNI North                        5          4    745.0   1170.0  tmax                  5
#    5 │      5  Terranora                        5          4     50.0    200.0  tmax                  5
#    6 │      6  QNI South                        6          5    910.0   1025.0  tmax                  6
#    7 │      7  CNSW->SNW North                  6          7   4730.0   4730.0  tmax                  6
#    8 │      8  CNSW->SNW South                  6          7   2720.0   2720.0  tmax                  6
#    9 │      9  VNI North                        8          6   2950.0   2590.0  tmin                  6
#   10 │     10  VNI South                        9          8   1000.0    400.0  tmin                  8
#   11 │     11  Heywood                          9         12    650.0    650.0  tmax                  9
#   12 │     12  SESA->CSA                       12         11    650.0    650.0  tmax                 12
#   13 │     13  Murraylink                       9         11    220.0    200.0  tmin                 11
#   14 │     14  Basslink                        10          9    594.0    478.0  tmin                  9
#   15 │     15  Project EnergyConnect            8         11    800.0    800.0  tmax                  8

# NOTE:
#   To get hypothetical tm (line maximum thermal limit), we need to get:
# 
#       t1, t2, pmax1, pmax2
# 
#   We use 1 as the winter and 2 as the summer. tmin and tmax in line data is from
# winter data, that is reverse and forward. We need to add tmin and tmax columns for
# summer first to fill the pmax2.
# 
#   Assume that we will only use tmax and tmax_summer (forward direction only). Then, we
# need to use t1 and t2, first from the :id_from then from the :id_to. We can ignore 
# the id_bus_limit and dir_limit for now.

"""
Infer branch maximum conductor temperature t_max from two derating points.

Scalar-safe (works with `ByRow`). Returns NaN when inputs are not usable
(NaN/Inf or p_max_1 ≈ p_max_2).
"""
function infer_branch_thermal_t_max(t_1::Real, t_2::Real, p_max_1::Real, p_max_2::Real; atol::Real=0.0)
    t1 = Float64(t_1)
    t2 = Float64(t_2)
    p1 = Float64(p_max_1)
    p2 = Float64(p_max_2)

    if !(isfinite(t1) && isfinite(t2) && isfinite(p1) && isfinite(p2))
        return NaN
    end

    p1_sq = p1^2
    p2_sq = p2^2
    denom = p1_sq - p2_sq
    if abs(denom) <= atol
        return NaN
    end

    return (p1_sq * t2 - p2_sq * t1) / denom
end

transform!(
    data["line"],
    :id_area_from => ByRow(id -> get(SiennaNEM.area_to_tref_summer, id, NaN)) => :tref_summer_from,
)
transform!(
    data["line"],
    :id_area_to => ByRow(id -> get(SiennaNEM.area_to_tref_summer, id, NaN)) => :tref_summer_to,
)
transform!(
    data["line"],
    :id_area_from => ByRow(id -> get(SiennaNEM.area_to_tref_winter, id, NaN)) => :tref_winter_from,
)
transform!(
    data["line"],
    :id_area_to => ByRow(id -> get(SiennaNEM.area_to_tref_winter, id, NaN)) => :tref_winter_to,
)
transform!(
    data["line"],
    [:tref_winter_from, :tref_summer_from, :tmax, :tmax_summer] =>
        ByRow((tw, ts, pw, ps) -> infer_branch_thermal_t_max(tw, ts, pw, ps)) =>
        :tm_from_fw,
)
transform!(
    data["line"],
    [:tref_winter_to, :tref_summer_to, :tmax, :tmax_summer] =>
        ByRow((tw, ts, pw, ps) -> infer_branch_thermal_t_max(tw, ts, pw, ps)) =>
        :tm_to_fw,
)
transform!(
    data["line"],
    [:tref_winter_from, :tref_summer_from, :tmin, :tmin_summer] =>
        ByRow((tw, ts, pw, ps) -> infer_branch_thermal_t_max(tw, ts, pw, ps)) =>
        :tm_from_rv,
)
transform!(
    data["line"],
    [:tref_winter_to, :tref_summer_to, :tmin, :tmin_summer] =>
        ByRow((tw, ts, pw, ps) -> infer_branch_thermal_t_max(tw, ts, pw, ps)) =>
        :tm_to_rv,
)

# tm calculation
# tm_default = 250.0  # default for high-temperature conductors
# tm_default = 100.0  # default for high, standard-temperature conductors
tm_default = 90.0  # default for high, standard-temperature conductors
transform!(
    data["line"],
    [:tm_from_fw, :tm_to_fw, :tm_from_rv, :tm_to_rv] =>
        ByRow((a, b, c, d) -> begin
            vals = Float64[a, b, c, d]
            good = filter(isfinite, vals)  # drops NaN/Inf
            isempty(good) ? tm_default : minimum(good)
        end) =>
        :tm,
)
# clip tm to <= tm_default
transform!(
    data["line"],
    :tm => ByRow(tm -> (isfinite(tm) ? min(tm, tm_default) : tm)) => :tm,
)
# HVDC Basslink (undersea cable) is not impacted by ambient temperature
data["line"][data["line"].id_lin .== 14, :tm] .= NaN
# HVDC Murraylink is specified by the operator to have a 46°C thermal limit
data["line"][data["line"].id_lin .== 13, :tm] .= 46

show(filter(:investment => ==(false), filter(:active => ==(true), data["line"]))[:, [
    :id_lin, :id_bus_from, :id_bus_to, :tmax, :tmax_summer, :tref_summer_from, :tref_winter_from, :tref_summer_to, :tref_winter_to, :tm_from_fw, :tm_to_fw, :tm_from_rv, :tm_to_rv, :tm
]], allrows=true, allcols=true)
# 15×13 DataFrame
#  Row │ id_lin  id_bus_from  id_bus_to  tmax     tmax_summer  tref_summer_from  tref_winter_from  tref_summer_to  tref_winter_to  tm_from_fw  tm_to_fw  tm_from_rv  tm_to_rv  
#      │ Int64   Int64        Int64      Float64  Float64      Float64           Float64           Float64         Float64         Float64     Float64   Float64     Float64   
# ─────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │      1            2          1   1400.0       1200.0              32.0              15.0            32.0            15.0     79.0769   79.0769     79.0769    79.0769
#    2 │      2            2          3   1050.0        700.0              32.0              15.0            32.0            15.0     45.6      45.6        46.7683    46.7683
#    3 │      3            4          2   1100.0       1100.0              32.0              15.0            32.0            15.0    NaN       NaN         NaN        NaN
#    4 │      4            5          4    745.0        745.0              32.0               9.0            32.0            15.0    NaN       NaN        2705.76    2008.26
#    5 │      5            5          4     50.0         50.0              32.0               9.0            32.0            15.0    NaN       NaN          61.5714    53.8571
#    6 │      6            6          5    910.0        910.0              32.0               9.0            32.0             9.0    NaN       NaN         139.108    139.108
#    7 │      7            6          7   4730.0       4490.0              32.0               9.0            32.0             9.0    241.546   241.546     241.546    241.546
#    8 │      8            6          7   2720.0       2540.0              32.0               9.0            32.0             9.0    188.725   188.725     188.725    188.725
#    9 │      9            8          6   2950.0       2700.0              32.0               9.0            32.0             9.0    150.704   150.704     125.381    125.381
#   10 │     10            9          8   1000.0       1000.0              32.0               8.0            32.0             9.0    NaN       NaN         NaN        NaN
#   11 │     11            9         12    650.0        650.0              32.0               8.0             7.7             1.2    NaN       NaN         NaN        NaN
#   12 │     12           12         11    650.0        650.0               7.7               1.2             7.7             1.2    NaN       NaN         NaN        NaN
#   13 │     13            9         11    220.0        220.0              32.0               8.0             7.7             1.2    NaN       NaN         NaN        NaN
#   14 │     14           10          9    594.0        594.0              35.0              11.0            32.0             8.0    NaN       NaN         NaN        NaN
#   15 │     15            8         11    800.0        NaN                32.0               9.0             7.7             1.2    NaN       NaN         NaN        NaN

"""
Branch thermal correction factor (CF) for ambient temperature.

CF = sqrt((t_max - t_ambient)/(t_max - t_ref)) for t_ambient < t_max, else 0
Returns NaN if inputs are not usable (NaN/Inf) or if t_max <= t_ref.
"""
function get_branch_thermal_correction_factor(t_ambient::Real, t_max::Real, t_ref::Real)
    ta = Float64(t_ambient)
    tm = Float64(t_max)
    tr = Float64(t_ref)

    if !(isfinite(ta) && isfinite(tm) && isfinite(tr))
        return NaN
    end
    if ta >= tm
        return 0.0
    end
    if tm <= tr
        return NaN
    end

    return sqrt((tm - ta) / (tm - tr))
end

# line-level tref (summer): highest tref between its terminal areas
transform!(
    data["line"],
    [:tref_summer_from, :tref_summer_to] => ByRow(max) => :tref_summer_line,
)

# CF at ambient using tm and tref_summer_line
t_ambient_eval = 42.0
transform!(
    data["line"],
    [:tm, :tref_summer_line] =>
        ByRow((tm, tr) -> get_branch_thermal_correction_factor(t_ambient_eval, tm, tr)) =>
        :cf_at_42C,
)
show(filter(:investment => ==(false), filter(:active => ==(true), data["line"]))[:, [
    :id_lin, :id_bus_from, :id_bus_to, :tmax_summer, :tref_summer_from, :tref_summer_to, :tref_summer_line, :tm, :cf_at_42C,
]], allrows=true, allcols=true)

"""
Branch thermal correction factor (CF) and slope (dCF/dT) for ambient temperature.

CF = sqrt((t_max - t_ambient)/(t_max - t_ref)) for t_ambient < t_max, else 0
dCF/dT = -(1 / (2 * sqrt(t_max - t_ref) * sqrt(t_max - t_ambient))) for t_ambient < t_max, else 0

Returns slope in % per °C if `scale=100.0` (default).
"""
function get_branch_thermal_correction_factor_slope(
    t_ambient::Real, t_max::Real, t_ref::Real;
    scale::Real = 100.0,
)
    ta = Float64(t_ambient)
    tm = Float64(t_max)
    tr = Float64(t_ref)

    # guard rails
    if !(isfinite(ta) && isfinite(tm) && isfinite(tr))
        return NaN
    end
    if ta >= tm
        return 0.0
    end
    if tm <= tr
        return NaN  # invalid reference (would be division by zero/complex)
    end

    return -1.0 / (2.0 * sqrt(tm - tr) * sqrt(tm - ta)) * scale
end

# Example: compute slope at 42°C ambient, using tm and the chosen line tref
transform!(
    data["line"],
    [:tm, :tref_summer_line] =>
        ByRow((tm, tr) -> get_branch_thermal_correction_factor_slope(t_ambient_eval, tm, tr)) =>
        :dcf_dt_pct_per_C_at_42C,
)
show(filter(:investment => ==(false), filter(:active => ==(true), data["line"]))[:, [
    :id_lin, :alias, :id_bus_from, :id_bus_to, :tmax_summer, :tref_summer_from, :tref_summer_to, :tref_summer_line, :tm, :cf_at_42C, :dcf_dt_pct_per_C_at_42C,
]], allrows=true, allcols=true)
