function plot_stacked(
    df::DataFrame,
    id_to_name::Union{OrderedDict, Dict};
    timecol::Symbol=:DateTime,
    title::String="Stacked Area Chart",
    yaxis_title::String="Value (MW)"
)
    """
    Plot stacked area chart for aggregated data (by bus, area, etc.).
    
    # Arguments
    - `df::DataFrame`: DataFrame with time column and data columns (named by ID)
    - `id_to_name::Union{OrderedDict, Dict}`: Mapping from IDs to display names
    - `timecol::Symbol`: Name of the time column (default: :DateTime)
    - `title::String`: Plot title
    - `yaxis_title::String`: Y-axis label (default: "Value (MW)")
    
    # Returns
    - PlotlyJS plot object
    """
    # Get time column
    time_data = df[!, timecol]
    
    # Get all data columns (excluding time column)
    data_cols = [col for col in names(df) if col != String(timecol)]
    
    # Create traces for each column
    traces = GenericTrace[]
    for col in reverse(data_cols)
        col_id = parse(Int, col)
        display_name = get(id_to_name, col_id, "ID $col")  # Fallback to ID if name not found
        
        trace = scatter(
            x=time_data,
            y=df[!, col],
            name=display_name,
            mode="lines",
            stackgroup="one",
            fillcolor="tozeroy"
        )
        push!(traces, trace)
    end
    
    # Create layout
    layout = Layout(
        title=title,
        xaxis_title="Time",
        yaxis_title=yaxis_title,
        hovermode="x unified",
        legend=attr(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02)
    )
    
    plot(traces, layout)
end
