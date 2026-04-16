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
add_area_data_col!(data["bus"], SiennaNEM.area_to_tref_summer; data_col=:tref_peak_demand)
add_area_data_col!(data["bus"], SiennaNEM.area_to_tref_summer; data_col=:tref_summer)
add_area_data_col!(data["bus"], SiennaNEM.area_to_tref_winter; data_col=:tref_winter)

data["bus"]
# 12×11 DataFrame
#  Row │ id_bus  name    alias                           active  latitude  longitude  id_area  area_name  tref_summer  tref_winter  tref_peak_demand 
#      │ Int64   String  String                          Bool    Float64   Float64    Int64    String     Float64      Float64      Float64          
# ─────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │      1  NQ      Northern Queensland               true  -17.7938    145.564        1  QLD               32.0         15.0              32.0
#    2 │      2  CQ      Central Queensland                true  -22.8242    149.404        1  QLD               32.0         15.0              32.0
#    3 │      3  GG      Gladstone Grid                    true  -23.8429    151.249        1  QLD               32.0         15.0              32.0
#    4 │      4  SQ      Southern Queensland               true  -27.4766    153.03         1  QLD               32.0         15.0              32.0
#    5 │      5  NNSW    Northern New South Wales          true  -30.5047    151.652        2  NSW               32.0          9.0              32.0
#    6 │      6  CNSW    Central New South Wales           true  -33.4833    150.158        2  NSW               32.0          9.0              32.0
#    7 │      7  SNW     Sydney, Newcastle & Wollongong    true  -33.865     151.209        2  NSW               32.0          9.0              32.0
#    8 │      8  SNSW    Southern New South Wales          true  -35.111     147.36         2  NSW               32.0          9.0              32.0
#    9 │      9  VIC     Victoria                          true  -37.7661    144.943        3  VIC               32.0          8.0              32.0
#   10 │     10  TAS     Tasmania                          true  -42.8806    147.325        4  TAS               35.0         11.0              35.0
#   11 │     11  CSA     Central South Australia           true  -34.8027    138.522        5  SA                 7.7          1.2               7.7
#   12 │     12  SESA    South East South Australia        true  -37.6047    140.837        5  SA                 7.7          1.2               7.7

# NOTE: We can see that for all buses, tref_peak_demand >= tref_summer >= tref_winter.
# We can use this as boundary in deciding line thermal derating in different seasons.

add_area_data_col!(data["area"], SiennaNEM.area_to_tref_summer; data_col=:tref_peak_demand)
add_area_data_col!(data["area"], SiennaNEM.area_to_tref_summer; data_col=:tref_summer)
add_area_data_col!(data["area"], SiennaNEM.area_to_tref_winter; data_col=:tref_winter)
data["area"]
# 5×8 DataFrame
#  Row │ id_area  name    peak_active_power  peak_reactive_power  max_pmax  tref_peak_demand  tref_summer  tref_winter 
#      │ Int64    String  Float64            Float64              Float64   Float64           Float64      Float64     
# ─────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │       1  QLD                   0.0                  0.0   2042.66              32.0         32.0         15.0
#    2 │       2  NSW                   0.0                  0.0   2345.46              32.0         32.0          9.0
#    3 │       3  VIC                   0.0                  0.0   5362.16              32.0         32.0          8.0
#    4 │       4  TAS                   0.0                  0.0    563.35              35.0         35.0         11.0
#    5 │       5  SA                    0.0                  0.0   2435.99               7.7          7.7          1.2

# NOTE:
#   1. tmax is forward power flow capacity
#   2. tmin is reverse power flow capacity
SiennaNEM.add_id_area_col!(data["line"], bus_to_area; bus_col=:id_bus_from, area_col=:id_area_from)
SiennaNEM.add_id_area_col!(data["line"], bus_to_area; bus_col=:id_bus_to, area_col=:id_area_to)
add_area_data_col!(data["line"], SiennaNEM.area_to_name; id_area_col=:id_area_from, data_col=:area_from)
add_area_data_col!(data["line"], SiennaNEM.area_to_name; id_area_col=:id_area_to, data_col=:area_to)
transform!(
    data["line"],
    :id_lin => ByRow(id -> get(line_to_tmin_summer, id, NaN)) => :tmin_summer,
)
transform!(
    data["line"],
    :id_lin => ByRow(id -> get(line_to_tmax_summer, id, NaN)) => :tmax_summer,
)
transform!(
    data["line"],
    :id_lin => ByRow(id -> get(line_to_tmin_peak_demand, id, NaN)) => :tmin_peak_demand,
)
transform!(
    data["line"],
    :id_lin => ByRow(id -> get(line_to_tmax_peak_demand, id, NaN)) => :tmax_peak_demand,
)

# NOTE: We should not fill NaN for new units without any data of summer and winter flow.
# for these cases, we should augment their line reference temperature instead later.
# Leaving them NaN is better as mark for later processing.

show(
    filter(
        :investment => ==(false), filter(:active => ==(true), data["line"])
    )[:, [:id_lin, :alias, :area_from, :area_to, :id_bus_from, :id_bus_to, :tmax, :tmin, :tmax_summer, :tmin_summer, :tmax_peak_demand, :tmin_peak_demand]],
    allrows=true, allcols=true
)
# 15×12 DataFrame
#  Row │ id_lin  alias                  area_from  area_to  id_bus_from  id_bus_to  tmax     tmin     tmax_summer  tmin_summer  tmax_peak_demand  tmin_peak_demand 
#      │ Int64   String                 String     String   Int64        Int64      Float64  Float64  Float64      Float64      Float64           Float64          
# ─────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │      1  CQ->NQ                 QLD        QLD                2          1   1400.0   1400.0       1200.0       1200.0            1200.0            1200.0
#    2 │      2  CQ->GG                 QLD        QLD                2          3   1050.0   1100.0        700.0        750.0             700.0             750.0
#    3 │      3  SQ->CQ                 QLD        QLD                4          2   1100.0   2100.0       1100.0       2100.0            1100.0            2100.0
#    4 │      4  QNI North              NSW        QLD                5          4    745.0   1170.0        745.0       1165.0             685.0            1205.0
#    5 │      5  Terranora              NSW        QLD                5          4     50.0    200.0         50.0        150.0               0.0             130.0
#    6 │      6  QNI South              NSW        NSW                6          5    910.0   1025.0        910.0        930.0             910.0             930.0
#    7 │      7  CNSW->SNW North        NSW        NSW                6          7   4730.0   4730.0       4490.0       4490.0            4490.0            4490.0
#    8 │      8  CNSW->SNW South        NSW        NSW                6          7   2720.0   2720.0       2540.0       2540.0            2540.0            2540.0
#    9 │      9  VNI North              NSW        NSW                8          6   2950.0   2590.0       2700.0       2320.0            2700.0            2320.0
#   10 │     10  VNI South              VIC        NSW                9          8   1000.0    400.0       1000.0        400.0             870.0             400.0
#   11 │     11  Heywood                VIC        SA                 9         12    650.0    650.0        650.0        650.0             650.0             650.0
#   12 │     12  SESA->CSA              SA         SA                12         11    650.0    650.0        650.0        650.0             650.0             650.0
#   13 │     13  Murraylink             VIC        SA                 9         11    220.0    200.0        220.0        200.0             220.0             100.0
#   14 │     14  Basslink               TAS        VIC               10          9    594.0    478.0        594.0        478.0             594.0             478.0
#   15 │     15  Project EnergyConnect  NSW        SA                 8         11    800.0    800.0        NaN          NaN               NaN               NaN

# NOTE: We don't actually need directional limit detection. We can ust use tmax and tmin
# independently.
# 
# transform!(
#     data["line"],
#     [:tmax, :tmin] =>
#         ByRow((tmax, tmin) -> (tmax <= tmin ? :tmax : :tmin)) =>
#             :dir_limit,
# )
# transform!(
#     data["line"],
#     [:dir_limit, :id_bus_from, :id_bus_to] =>
#         ByRow((dir, id_from, id_to) -> (dir === :tmax ? id_from : id_to)) =>
#             :id_bus_limit,
# )
# SiennaNEM.add_id_area_col!(
#     data["line"], bus_to_area;
#     bus_col=:id_bus_limit, area_col=:id_area_limit
# )

# add tech
transform!(
    data["line"],
    :id_lin => ByRow(id -> get(line_to_tech, id, "ac_oh")) => :tech,
)
show(
    filter(
        :investment => ==(false), filter(:active => ==(true), data["line"])
    )[:, [:id_lin, :alias, :area_from, :area_to, :tech]],
    allrows=true, allcols=true
)
# 15×5 DataFrame
#  Row │ id_lin  alias                  area_from  area_to  tech   
#      │ Int64   String                 String     String   String 
# ─────┼───────────────────────────────────────────────────────────
#    1 │      1  CQ->NQ                 QLD        QLD      ac_oh
#    2 │      2  CQ->GG                 QLD        QLD      ac_oh
#    3 │      3  SQ->CQ                 QLD        QLD      ac_oh
#    4 │      4  QNI North              NSW        QLD      ac_oh
#    5 │      5  Terranora              NSW        QLD      dc_oh
#    6 │      6  QNI South              NSW        NSW      ac_oh
#    7 │      7  CNSW->SNW North        NSW        NSW      ac_oh
#    8 │      8  CNSW->SNW South        NSW        NSW      ac_oh
#    9 │      9  VNI North              NSW        NSW      ac_oh
#   10 │     10  VNI South              VIC        NSW      ac_oh
#   11 │     11  Heywood                VIC        SA       ac_oh
#   12 │     12  SESA->CSA              SA         SA       ac_oh
#   13 │     13  Murraylink             VIC        SA       dc_oh
#   14 │     14  Basslink               TAS        VIC      dc_ss
#   15 │     15  Project EnergyConnect  NSW        SA       ac_oh

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
# need to use t1 and t2, first from the :id_from then from the :id_to. 

# add tref
transform!(
    data["line"],
    :id_area_from =>
        ByRow(id -> get(SiennaNEM.area_to_tref_peak_demand, id, NaN)) =>
            :tref_peak_demand_from,
)
transform!(
    data["line"],
    :id_area_to =>
        ByRow(id -> get(SiennaNEM.area_to_tref_peak_demand, id, NaN)) =>
            :tref_peak_demand_to,
)
transform!(
    data["line"],
    :id_area_from =>
        ByRow(id -> get(SiennaNEM.area_to_tref_summer, id, NaN)) =>
            :tref_summer_from,
)
transform!(
    data["line"],
    :id_area_to =>
        ByRow(id -> get(SiennaNEM.area_to_tref_summer, id, NaN)) =>
            :tref_summer_to,
)
transform!(
    data["line"],
    :id_area_from =>
        ByRow(id -> get(SiennaNEM.area_to_tref_winter, id, NaN)) =>
            :tref_winter_from,
)
transform!(
    data["line"],
    :id_area_to =>
        ByRow(id -> get(SiennaNEM.area_to_tref_winter, id, NaN)) =>
            :tref_winter_to,
)

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

    tm = (p1_sq * t2 - p2_sq * t1) / denom
    if tm < max(t1, t2)
        return NaN
    end
    return tm
end

# for ta <= tref_winter: cf = 1, use tmax and tmin, return (tmax_winter, tmin_winter)
# for tref_winter < ta <= tref_summer: tm1, use tmax_summer and tmin_summer, return (tmax_winter, tmin_winter) * cf 
# for tref_summer < ta <= tref_peak_demand: tm2, use tmax_summer and tmin_summer, return (tmax_summer, tmin_winter) * cf (tmax_summer, tmin_winter)
# for tref_peak_demand < ta: tm3, use tmax_peak_demand and tmin_peak_demand, return (tmax_peak_demand, tmin_winter) * cf 
# tm3 is min(tm2, tm_cap)
# 
# NOTE: Sometimes, tm = NaN due to no impact of temperature on the line rating for a
# specific piecewise region. In the case of tm == NaN:
# 
#   Region 1: return (tmax_winter, tmin_winter)
#   Region 2: return (tmax_winter, tmin_winter)
#   Region 3: return (tmax_summer, tmin_winter)
#   Region 4: return (tmax_peak_demand, tmin_winter) (actually impossible due to tm_cap)

# tm_cap = 250.0  # default for high-temperature conductors (ACSS)
# tm_cap = 100.0  # default for high, standard-temperature conductors (ACSR)
tm_cap = 90.0  # default for high, standard-temperature conductors

# for the forward flow: tmax
transform!(
    data["line"],
    [:tref_winter_from, :tref_summer_from, :tmax, :tmax_summer] =>
        ByRow((tw, ts, pw, ps) -> infer_branch_thermal_t_max(tw, ts, pw, ps)) =>
            :tm1_from_tmax,
)
transform!(
    data["line"],
    [:tref_winter_to, :tref_summer_to, :tmax, :tmax_summer] =>
        ByRow((tw, ts, pw, ps) -> infer_branch_thermal_t_max(tw, ts, pw, ps)) =>
            :tm1_to_tmax,
)
transform!(
    data["line"],
    [:tref_summer_from, :tref_peak_demand_from, :tmax_summer, :tmax_peak_demand] =>
        ByRow((ts, tp, ps, pp) -> infer_branch_thermal_t_max(ts, tp, ps, pp)) =>
            :tm2_from_tmax,
)
transform!(
    data["line"],
    [:tref_summer_to, :tref_peak_demand_to, :tmax_summer, :tmax_peak_demand] =>
        ByRow((ts, tp, ps, pp) -> infer_branch_thermal_t_max(ts, tp, ps, pp)) =>
            :tm2_to_tmax,
)
transform!(
    data["line"],
    :tm2_from_tmax =>
        ByRow(tm2 -> (isfinite(tm2) ? min(tm2, tm_cap) : tm_cap)) =>
            :tm3_from_tmax,
)
transform!(
    data["line"],
    :tm2_to_tmax =>
        ByRow(tm2 -> (isfinite(tm2) ? min(tm2, tm_cap) : tm_cap)) =>
            :tm3_to_tmax,
)

# for the reverse flow: tmin
transform!(
    data["line"],
    [:tref_winter_from, :tref_summer_from, :tmin, :tmin_summer] =>
        ByRow((tw, ts, pw, ps) -> infer_branch_thermal_t_max(tw, ts, pw, ps)) =>
            :tm1_from_tmin,
)
transform!(
    data["line"],
    [:tref_winter_to, :tref_summer_to, :tmin, :tmin_summer] =>
        ByRow((tw, ts, pw, ps) -> infer_branch_thermal_t_max(tw, ts, pw, ps)) =>
            :tm1_to_tmin,
)
transform!(
    data["line"],
    [:tref_summer_from, :tref_peak_demand_from, :tmin_summer, :tmin_peak_demand] =>
        ByRow((ts, tp, ps, pp) -> infer_branch_thermal_t_max(ts, tp, ps, pp)) =>
            :tm2_from_tmin,
)
transform!(
    data["line"],
    [:tref_summer_to, :tref_peak_demand_to, :tmin_summer, :tmin_peak_demand] =>
        ByRow((ts, tp, ps, pp) -> infer_branch_thermal_t_max(ts, tp, ps, pp)) =>
            :tm2_to_tmin,
)
transform!(
    data["line"],
    :tm2_from_tmin =>
        ByRow(tm2 -> (isfinite(tm2) ? min(tm2, tm_cap) : tm_cap)) =>
            :tm3_from_tmin,
)
transform!(
    data["line"],
    :tm2_to_tmin =>
        ByRow(tm2 -> (isfinite(tm2) ? min(tm2, tm_cap) : tm_cap)) =>
            :tm3_to_tmin,
)

# Combine "from/to" without mixing directions:
# - forward (tmax): take min(tm*_from_tmax, tm*_to_tmax)
# - reverse (tmin): take min(tm*_from_tmin, tm*_to_tmin)
# If both are non-finite -> NaN (keep missingness)
transform!(
    data["line"],
    [:tm1_from_tmax, :tm1_to_tmax] =>
        ByRow((a, b) -> begin
            vals = (Float64(a), Float64(b))
            good = filter(isfinite, vals)
            isempty(good) ? NaN : minimum(good)
        end) => :tm1_tmax,
)
transform!(
    data["line"],
    [:tm2_from_tmax, :tm2_to_tmax] =>
        ByRow((a, b) -> begin
            vals = (Float64(a), Float64(b))
            good = filter(isfinite, vals)
            isempty(good) ? NaN : minimum(good)
        end) => :tm2_tmax,
)
transform!(
    data["line"],
    [:tm3_from_tmax, :tm3_to_tmax] =>
        ByRow((a, b) -> min(Float64(a), Float64(b))) => :tm3_tmax,
)

transform!(
    data["line"],
    [:tm1_from_tmin, :tm1_to_tmin] =>
        ByRow((a, b) -> begin
            vals = (Float64(a), Float64(b))
            good = filter(isfinite, vals)
            isempty(good) ? NaN : minimum(good)
        end) => :tm1_tmin,
)
transform!(
    data["line"],
    [:tm2_from_tmin, :tm2_to_tmin] =>
        ByRow((a, b) -> begin
            vals = (Float64(a), Float64(b))
            good = filter(isfinite, vals)
            isempty(good) ? NaN : minimum(good)
        end) => :tm2_tmin,
)
transform!(
    data["line"],
    [:tm3_from_tmin, :tm3_to_tmin] =>
        ByRow((a, b) -> min(Float64(a), Float64(b))) => :tm3_tmin,
)

show(filter(:investment => ==(false), filter(:active => ==(true), data["line"]))[:, [
        :id_lin, :alias, :area_from, :area_to, :tm1_tmax, :tm2_tmax, :tm3_tmax, :tm1_tmin, :tm2_tmin, :tm3_tmin
    ]],
    allrows=true, allcols=true
)

# fix tm for HVDC lines:
cols_tm = [:tm1_tmax, :tm2_tmax, :tm3_tmax, :tm1_tmin, :tm2_tmin, :tm3_tmin]
cols_tm_tmax = [:tm1_tmax, :tm2_tmax, :tm3_tmax]
# HVDC Terranora should be limited to:
#   1. To 46°C, the same as Murraylink for reverse flow (tmin)
#   2. To 37.0°C for forward flow (tmax) as specified by the operator
mask_terranora = data["line"].id_lin .== 5
data["line"][mask_terranora, cols_tm] .=
    ifelse.(
        isfinite.(data["line"][mask_terranora, cols_tm]),
        data["line"][mask_terranora, cols_tm],
        46.0
    )
data["line"][mask_terranora, cols_tm] .=
    min.(data["line"][mask_terranora, cols_tm], 46.0)
data["line"][mask_terranora, cols_tm_tmax] .=
    min.(data["line"][mask_terranora, cols_tm_tmax], 37.0)
# HVDC Murraylink is specified by the operator to have a 46°C thermal limit
data["line"][data["line"].id_lin.==13, cols_tm] .= 46.0
# HVDC Basslink (undersea cable) is not impacted by ambient temperature
data["line"][data["line"].id_lin.==14, cols_tm] .= NaN

# NOTE: Now, we need to add tref for each line as we can't use nodes. For this,
# we assume the lowest tref among the two ends as the line tref to be conservative.

transform!(
    data["line"],
    [:tref_peak_demand_from, :tref_peak_demand_to] =>
        ByRow((a, b) -> min(Float64(a), Float64(b))) => :tref_peak_demand,
)
transform!(
    data["line"],
    [:tref_summer_from, :tref_summer_to] =>
        ByRow((a, b) -> min(Float64(a), Float64(b))) => :tref_summer,
)
transform!(
    data["line"],
    [:tref_winter_from, :tref_winter_to] =>
        ByRow((a, b) -> min(Float64(a), Float64(b))) => :tref_winter,
)

# New lines without any data of summer and winter flow should be derated based on their
# (default) line reference temperature.
cols_missing_ratings = [:tmax_summer, :tmax_peak_demand, :tmin_summer, :tmin_peak_demand]
mask_no_seasonal_data =
    (data["line"].tech .== "ac_oh") .&
    reduce(.&, (.!isfinite.(data["line"][!, c]) for c in cols_missing_ratings))
data["line"][mask_no_seasonal_data, cols_tm] .= constant_temperature["ac_oh_tm"]
data["line"][mask_no_seasonal_data, [:tref_peak_demand, :tref_summer, :tref_winter]] .=
    constant_temperature["ac_oh_tref"]
data["line"][mask_no_seasonal_data, :tmax_summer] .= data["line"][mask_no_seasonal_data, :tmax]
data["line"][mask_no_seasonal_data, :tmax_peak_demand] .= data["line"][mask_no_seasonal_data, :tmax]
data["line"][mask_no_seasonal_data, :tmin_summer] .= data["line"][mask_no_seasonal_data, :tmin]
data["line"][mask_no_seasonal_data, :tmin_peak_demand] .= data["line"][mask_no_seasonal_data, :tmin]

show(filter(:investment => ==(false), filter(:active => ==(true), data["line"]))[:, [
        :id_lin, :alias, :area_from, :area_to, :tref_peak_demand, :tref_summer, :tref_winter, :tm1_tmax, :tm2_tmax, :tm3_tmax, :tm1_tmin, :tm2_tmin, :tm3_tmin
    ]],
    allrows=true, allcols=true
)

# 15×13 DataFrame
#  Row │ id_lin  alias                  area_from  area_to  tref_peak_demand  tref_summer  tref_winter  tm1_tmax  tm2_tmax  tm3_tmax  tm1_tmin   tm2_tmin  tm3_tmin 
#      │ Int64   String                 String     String   Float64           Float64      Float64      Float64   Float64   Float64   Float64    Float64   Float64  
# ─────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │      1  CQ->NQ                 QLD        QLD                  37.0         32.0         15.0   79.0769  NaN        90.0       79.0769     NaN        90.0
#    2 │      2  CQ->GG                 QLD        QLD                  37.0         32.0         15.0   45.6     NaN        90.0       46.7683     NaN        90.0
#    3 │      3  SQ->CQ                 QLD        QLD                  37.0         32.0         15.0  NaN       NaN        90.0      NaN          NaN        90.0
#    4 │      4  QNI North              NSW        QLD                  37.0         32.0          9.0  NaN        64.3441   64.3441  2008.26       NaN        90.0
#    5 │      5  Terranora              NSW        QLD                  37.0         32.0          9.0   37.0      37.0      37.0       46.0         46.0      46.0
#    6 │      6  QNI South              NSW        NSW                  42.0         32.0          9.0  NaN       NaN        90.0      139.108      NaN        90.0
#    7 │      7  CNSW->SNW North        NSW        NSW                  42.0         32.0          9.0  241.546   NaN        90.0      241.546      NaN        90.0
#    8 │      8  CNSW->SNW South        NSW        NSW                  42.0         32.0          9.0  188.725   NaN        90.0      188.725      NaN        90.0
#    9 │      9  VNI North              NSW        NSW                  42.0         32.0          9.0  150.704   NaN        90.0      125.381      NaN        90.0
#   10 │     10  VNI South              VIC        NSW                  41.0         32.0          8.0  NaN        69.0218   69.0218   NaN          NaN        90.0
#   11 │     11  Heywood                VIC        SA                   41.0         32.0          8.0  NaN       NaN        90.0      NaN          NaN        90.0
#   12 │     12  SESA->CSA              SA         SA                   43.0         35.0         11.0  NaN       NaN        90.0      NaN          NaN        90.0
#   13 │     13  Murraylink             VIC        SA                   41.0         32.0          8.0   46.0      46.0      46.0       46.0         46.0      46.0
#   14 │     14  Basslink               TAS        VIC                   7.7          7.7          1.2  NaN       NaN       NaN        NaN          NaN       NaN
#   15 │     15  Project EnergyConnect  NSW        SA                   20.0         20.0         20.0   90.0      90.0      90.0       90.0         90.0      90.0

"""
    get_branch_thermal_capacity_ac_oh(
        ta,
        t1, t2, t3,
        p1, p2, p3,
        tm1, tm2, tm3,
    ) -> Float64

Compute the thermally derated branch capacity at ambient temperature `ta`,
using a 4-region piecewise model. Call once for forward flow (tmax) and
once for reverse flow (tmin).

    t1 = tref_winter, t2 = tref_summer, t3 = tref_peak_demand
    p1 = capacity at t1 (winter), p2 = capacity at t2 (summer), p3 = capacity at t3 (peak demand)
    tm1, tm2, tm3 = inferred conductor temperatures for regions 2, 3, 4

Regions:

| Region | Condition        | tm   | Base | CF reference |
|--------|------------------|------|------|--------------|
| 1      | `ta ≤ t1`        | —    | `p1` | —            |
| 2      | `t1 < ta ≤ t2`   | `tm1`| `p1` | `t1`         |
| 3      | `t2 < ta ≤ t3`   | `tm2`| `p2` | `t2`         |
| 4      | `ta > t3`        | `tm3`| `p3` | `t3`         |

CF shape:

    CF(ta) = sqrt(max(tm - ta, 0) / (tm - t_reference))

NaN fallback (when `tm` is NaN, no derating applies in that region):

| Region | NaN fallback |
|--------|--------------|
| 2      | `p1`         |
| 3      | `p2`         |
| 4      | `p3`         |
"""
function get_branch_thermal_capacity_ac_oh(
    ta::Real,
    t1::Real, t2::Real, t3::Real,
    p1::Real, p2::Real, p3::Real,
    tm1::Real, tm2::Real, tm3::Real,
)
    ta = Float64(ta)

    function cf(tm::Float64, t_reference::Float64)
        isnan(tm) && return NaN
        return sqrt(max(tm - ta, 0.0) / (tm - t_reference))
    end

    if ta <= t1
        return Float64(p1)

    elseif ta <= t2
        c = cf(Float64(tm1), Float64(t1))
        return isnan(c) ? Float64(p1) : Float64(p1) * c

    elseif ta <= t3
        c = cf(Float64(tm2), Float64(t2))
        return isnan(c) ? Float64(p2) : Float64(p2) * c

    else
        c = cf(Float64(tm3), Float64(t3))
        return isnan(c) ? Float64(p3) : Float64(p3) * c
    end
end

"""
    get_branch_thermal_capacity_dc_oh(ta, tm, p) -> Float64

Compute derated capacity for DC overhead (`dc_oh`) lines.

- `ta ≤ DC_OH_BASE_TEMP`          : return `p` (no reduction)
- `DC_OH_BASE_TEMP < ta ≤ tm`     : return `p * (1 - DC_OH_RATE * (ta - DC_OH_BASE_TEMP))`
- `ta > tm`                        : return `0.0`
"""
function get_branch_thermal_capacity_dc_oh(ta::Real, tm::Real, p::Real)
    ta = Float64(ta)
    tm = Float64(tm)
    p = Float64(p)

    if ta <= constant_temperature["dc_oh_tref"]
        return p
    elseif ta <= tm
        return p * (1.0 - constant_temperature["dc_oh_derating_rate"] * (ta - constant_temperature["dc_oh_tref"]))
    else
        return 0.0
    end
end

# ambient temperature for derating (°C)
# ta = 25.0  # for testing mild temperature
# ta = 36.0  # for testing summer temperature
# ta = 42.0  # for testing high temperature
ta = 47.0  # for testing extreme temperature

transform!(
    data["line"],
    [:tech,
        :tref_winter, :tref_summer, :tref_peak_demand,
        :tmax, :tmax_summer, :tmax_peak_demand,
        :tm1_tmax, :tm2_tmax, :tm3_tmax] =>
        ByRow((tech, args...) -> begin
            if tech == "ac_oh"
                get_branch_thermal_capacity_ac_oh(ta, args...)
            elseif tech == "dc_oh"
                # use tm3 as the conductor limit, p1 (tmax) as base capacity
                t1, t2, t3, p1, p2, p3, tm1, tm2, tm3 = args
                get_branch_thermal_capacity_dc_oh(ta, tm3, p1)
            else  # dc_ss
                Float64(args[4])  # p1 = tmax, no derating
            end
        end) => :tmax_derated,
)
transform!(
    data["line"],
    [:tech,
        :tref_winter, :tref_summer, :tref_peak_demand,
        :tmin, :tmin_summer, :tmin_peak_demand,
        :tm1_tmin, :tm2_tmin, :tm3_tmin] =>
        ByRow((tech, args...) -> begin
            if tech == "ac_oh"
                get_branch_thermal_capacity_ac_oh(ta, args...)
            elseif tech == "dc_oh"
                t1, t2, t3, p1, p2, p3, tm1, tm2, tm3 = args
                get_branch_thermal_capacity_dc_oh(ta, tm3, p1)
            else  # dc_ss
                Float64(args[4])  # p1 = tmin, no derating
            end
        end) => :tmin_derated,
)

show(
    filter(:investment => ==(false), filter(:active => ==(true), data["line"]))[:, [
        :id_lin, :alias, :area_from, :area_to, :tref_peak_demand, :tref_summer, :tref_winter, :tm1_tmax, :tm2_tmax, :tm3_tmax, :tm1_tmin, :tm2_tmin, :tm3_tmin, :tmax, :tmax_peak_demand, :tmax_derated, :tmin, :tmin_derated
    ]],
    allrows=true, allcols=true
)
# 15×18 DataFrame
#  Row │ id_lin  alias                  area_from  area_to  tref_peak_demand  tref_summer  tref_winter  tm1_tmax  tm2_tmax  tm3_tmax  tm1_tmin   tm2_tmin  tm3_tmin  tmax     tmax_peak_demand  tmax_derated  tmin     tmin_derated 
#      │ Int64   String                 String     String   Float64           Float64      Float64      Float64   Float64   Float64   Float64    Float64   Float64   Float64  Float64           Float64       Float64  Float64      
# ─────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │      1  CQ->NQ                 QLD        QLD                  37.0         32.0         15.0   79.0769  NaN        90.0       79.0769     NaN        90.0   1400.0            1200.0      1080.88    1400.0      1080.88
#    2 │      2  CQ->GG                 QLD        QLD                  37.0         32.0         15.0   45.6     NaN        90.0       46.7683     NaN        90.0   1050.0             700.0       630.513   1100.0       675.55
#    3 │      3  SQ->CQ                 QLD        QLD                  37.0         32.0         15.0  NaN       NaN        90.0      NaN          NaN        90.0   1100.0            1100.0       990.807   2100.0      1891.54
#    4 │      4  QNI North              NSW        QLD                  37.0         32.0          9.0  NaN        64.3441   64.3441  2008.26       NaN        90.0    745.0             685.0       545.55    1170.0      1085.38
#    5 │      5  Terranora              NSW        QLD                  37.0         32.0          9.0   37.0      37.0      37.0       46.0         46.0      46.0     50.0               0.0         0.0      200.0         0.0
#    6 │      6  QNI South              NSW        NSW                  42.0         32.0          9.0  NaN       NaN        90.0      139.108      NaN        90.0    910.0             910.0       861.301   1025.0       880.231
#    7 │      7  CNSW->SNW North        NSW        NSW                  42.0         32.0          9.0  241.546   NaN        90.0      241.546      NaN        90.0   4730.0            4490.0      4249.72    4730.0      4249.72
#    8 │      8  CNSW->SNW South        NSW        NSW                  42.0         32.0          9.0  188.725   NaN        90.0      188.725      NaN        90.0   2720.0            2540.0      2404.07    2720.0      2404.07
#    9 │      9  VNI North              NSW        NSW                  42.0         32.0          9.0  150.704   NaN        90.0      125.381      NaN        90.0   2950.0            2700.0      2555.51    2590.0      2195.84
#   10 │     10  VNI South              VIC        NSW                  41.0         32.0          8.0  NaN        69.0218   69.0218   NaN          NaN        90.0   1000.0             870.0       771.254    400.0       374.711
#   11 │     11  Heywood                VIC        SA                   41.0         32.0          8.0  NaN       NaN        90.0      NaN          NaN        90.0    650.0             650.0       608.905    650.0       608.905
#   12 │     12  SESA->CSA              SA         SA                   43.0         35.0         11.0  NaN       NaN        90.0      NaN          NaN        90.0    650.0             650.0       621.725    650.0       621.725
#   13 │     13  Murraylink             VIC        SA                   41.0         32.0          8.0   46.0      46.0      46.0       46.0         46.0      46.0    220.0             220.0         0.0      200.0         0.0
#   14 │     14  Basslink               TAS        VIC                   7.7          7.7          1.2  NaN       NaN       NaN        NaN          NaN       NaN      594.0             594.0       594.0      478.0       478.0
#   15 │     15  Project EnergyConnect  NSW        SA                   20.0         20.0         20.0   90.0      90.0      90.0       90.0         90.0      90.0    800.0             800.0       627.011    800.0       627.011

println("Ambient temperature = $(ta)°C:")
show(
    filter(:investment => ==(false), filter(:active => ==(true), data["line"]))[:, [
        :id_lin, :alias, :tref_winter, :tref_summer, :tref_peak_demand, :tm1_tmax, :tm2_tmax, :tm3_tmax, :tmax, :tmax_summer, :tmax_peak_demand, :tmax_derated,
    ]],
    allrows=true, allcols=true
)

show(
    filter(:investment => ==(false), filter(:active => ==(true), data["line"]))[:, [
        :id_lin, :alias, :tref_winter, :tref_summer, :tref_peak_demand, :tm1_tmin, :tm2_tmin, :tm3_tmin, :tmin, :tmin_summer, :tmin_peak_demand, :tmin_derated
    ]],
    allrows=true, allcols=true
)

"""
    get_branch_thermal_capacity(ta_df, line_df, cols; tech_col=:tech) -> DataFrame

Agnostic DataFrame version of thermal derating.

- `ta_df[:value]` is ambient temperature (°C)
- `cols` must be a 9-element vector/tuple of Symbols in this order:
    (t1, t2, t3, p1, p2, p3, tm1, tm2, tm3)
- joins `ta_df` with `line_df` on `:id_lin`
- overwrites `:value` with derated MW
- returns `:id, :id_lin, :scenario, :date, :value`
"""
function get_branch_thermal_capacity(
    ta_df::DataFrame,
    line_df::DataFrame,
    cols;
    tech_col::Symbol = :tech,
)
    t1, t2, t3, p1, p2, p3, tm1, tm2, tm3 = cols

    df = leftjoin(ta_df, line_df; on=:id_lin)

    transform!(
        df,
        [tech_col, t1, t2, t3, p1, p2, p3, tm1, tm2, tm3, :value] =>
        ByRow((tech, t1v, t2v, t3v, p1v, p2v, p3v, tm1v, tm2v, tm3v, ta) -> begin
            if tech == "ac_oh"
                get_branch_thermal_capacity_ac_oh(ta, t1v, t2v, t3v, p1v, p2v, p3v, tm1v, tm2v, tm3v)
            elseif tech == "dc_oh"
                get_branch_thermal_capacity_dc_oh(ta, tm3v, p1v)
            else
                Float64(p1v)
            end
        end) => :value,
    )

    select(df, :id, :id_lin, :scenario, :date, :value)
end

ta_df = DataFrame(
    id=1:8,
    id_lin=[15, 15, 15, 15, 15, 15, 1, 1],
    scenario=[1, 2, 3, 1, 2, 3, 1, 1],
    date=DateTime.([
        "2024-07-01", "2024-07-01", "2024-07-01",
        "2026-07-01", "2026-07-01", "2026-07-01",
        "2030-01-01", "2030-06-01",
    ]),
    value=[15.0, 15.0, 15.0, 40.0, 40.0, 40.0, 40.0, 40.0],
)
ta_df
# 8×5 DataFrame
#  Row │ id     id_lin  scenario  date                 value   
#      │ Int64  Int64   Int64     DateTime             Float64 
# ─────┼───────────────────────────────────────────────────────
#    1 │     1      15         1  2024-07-01T00:00:00     15.0
#    2 │     2      15         2  2024-07-01T00:00:00     15.0
#    3 │     3      15         3  2024-07-01T00:00:00     15.0
#    4 │     4      15         1  2026-07-01T00:00:00     40.0
#    5 │     5      15         2  2026-07-01T00:00:00     40.0
#    6 │     6      15         3  2026-07-01T00:00:00     40.0
#    7 │     7       1         1  2030-01-01T00:00:00     40.0
#    8 │     8       1         1  2030-06-01T00:00:00     40.0

cap_tmax = get_branch_thermal_capacity(
    ta_df, data["line"],
    (:tref_winter, :tref_summer, :tref_peak_demand,
     :tmax, :tmax_summer, :tmax_peak_demand,
     :tm1_tmax, :tm2_tmax, :tm3_tmax)
)
# 8×5 DataFrame
#  Row │ id     id_lin  scenario  date                 value    
#      │ Int64  Int64   Int64     DateTime             Float64  
# ─────┼────────────────────────────────────────────────────────
#    1 │     7       1         1  2030-01-01T00:00:00  1165.54
#    2 │     8       1         1  2030-06-01T00:00:00  1165.54
#    3 │     1      15         1  2024-07-01T00:00:00   800.0
#    4 │     2      15         2  2024-07-01T00:00:00   800.0
#    5 │     3      15         3  2024-07-01T00:00:00   800.0
#    6 │     4      15         1  2026-07-01T00:00:00   676.123
#    7 │     5      15         2  2026-07-01T00:00:00   676.123
#    8 │     6      15         3  2026-07-01T00:00:00   676.123

cap_tmin = get_branch_thermal_capacity(
    ta_df, data["line"],
    (:tref_winter, :tref_summer, :tref_peak_demand,
     :tmin, :tmin_summer, :tmin_peak_demand,
     :tm1_tmin, :tm2_tmin, :tm3_tmin)
)
# 8×5 DataFrame
#  Row │ id     id_lin  scenario  date                 value    
#      │ Int64  Int64   Int64     DateTime             Float64  
# ─────┼────────────────────────────────────────────────────────
#    1 │     7       1         1  2030-01-01T00:00:00  1165.54
#    2 │     8       1         1  2030-06-01T00:00:00  1165.54
#    3 │     1      15         1  2024-07-01T00:00:00   800.0
#    4 │     2      15         2  2024-07-01T00:00:00   800.0
#    5 │     3      15         3  2024-07-01T00:00:00   800.0
#    6 │     4      15         1  2026-07-01T00:00:00   676.123
#    7 │     5      15         2  2026-07-01T00:00:00   676.123
#    8 │     6      15         3  2026-07-01T00:00:00   676.123
