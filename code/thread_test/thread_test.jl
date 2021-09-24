using DataFrames
using CSV
using StatsBase

function main()
    d = CSV.read("example_df.csv", DataFrame, debug=true)
    combine(
        groupby(d, :station),
        :avg_occ => sum => :total_occ,
        :total_flow => sum => :total_flow,
        :lane_type => first => :station_type,
        :freeway_number => first => :freeway_number,
        :direction => first => :direction
    )
end

@time main()