# NOTE:
#   data["area"] is not from .csv and .arrow, but from
# SiennaNEM.read_data.add_area_df!

using SiennaNEM

bus_to_area = SiennaNEM.get_map_from_df(data["bus"], :id_bus, :id_area)
SiennaNEM.add_id_area_col!(data["generator"], bus_to_area)
SiennaNEM.add_area_df!(data)

"""
    add_area_name_col!(
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
function add_area_name_col!(
    df;
    id_area_col::Symbol=:id_area,
    area_name_col::Symbol=:area_name,
)
    # NOTE: area_to_name is a constant from SiennaNEM.const
    df[!, area_name_col] = [SiennaNEM.area_to_name[id] for id in df[!, id_area_col]]
end

add_area_name_col!(data["bus"])

# data["bus"]
# 12×8 DataFrame
#  Row │ id_bus  name    alias                           active  latitude  longitude  id_area  area_name 
#      │ Int64   String  String                          Bool    Float64   Float64    Int64    String    
# ─────┼─────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │      1  NQ      Northern Queensland               true  -17.7938    145.564        1  QLD
#    2 │      2  CQ      Central Queensland                true  -22.8242    149.404        1  QLD
#    3 │      3  GG      Gladstone Grid                    true  -23.8429    151.249        1  QLD
#    4 │      4  SQ      Southern Queensland               true  -27.4766    153.03         1  QLD
#    5 │      5  NNSW    Northern New South Wales          true  -30.5047    151.652        2  NSW
#    6 │      6  CNSW    Central New South Wales           true  -33.4833    150.158        2  NSW
#    7 │      7  SNW     Sydney, Newcastle & Wollongong    true  -33.865     151.209        2  NSW
#    8 │      8  SNSW    Southern New South Wales          true  -35.111     147.36         2  NSW
#    9 │      9  VIC     Victoria                          true  -37.7661    144.943        3  VIC
#   10 │     10  TAS     Tasmania                          true  -42.8806    147.325        4  TAS
#   11 │     11  CSA     Central South Australia           true  -34.8027    138.522        5  SA
#   12 │     12  SESA    South East South Australia        true  -37.6047    140.837        5  SA

data["area"]
# 5×5 DataFrame
#  Row │ id_area  name    peak_active_power  peak_reactive_power  max_pmax 
#      │ Int64    String  Float64            Float64              Float64  
# ─────┼───────────────────────────────────────────────────────────────────
#    1 │       1  QLD                   0.0                  0.0   2042.66
#    2 │       2  NSW                   0.0                  0.0   2345.46
#    3 │       3  VIC                   0.0                  0.0   5362.16
#    4 │       4  TAS                   0.0                  0.0    563.35
#    5 │       5  SA                    0.0                  0.0   2435.99
