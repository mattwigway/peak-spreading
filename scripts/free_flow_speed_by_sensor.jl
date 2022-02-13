# Calculate the free-flow speed for every sensor in the network
# From the Highway Capacity Manual: "under base conditions, the FFS on freeways is expected to prevail
# at flow rates below 1000 passenger cars per hour per lane. This characteristic simplifies and allows
# measurement of free-flow speeds directly from sensor data."
# We compute the (approximate) median speed at each sensor when flow rates are at or below 1000 pc/hr/lane.
# We use median rather than mean because this distribution is likely to be skewed - many things could lower the
# free-flow speed (adverse weather conditions, crashes, etc). Few things are likely to raise the free flow speed,
# as it is theoretically the maximum speed generally attained on the road.

using OnlineStats, KFactors, ProgressMeter, CSV, DataFrames, ArgParse, Dates

s = ArgParseSettings()

@add_arg_table s begin
    "data_dir"
        help = "Directory with data"
end

function update_ffs(day, results)
    for row in day
        # flow in 5 minute period is 1/12 hourly flow
        if row.pct_obs == 100 && !ismissing(row.avg_speed_mph) && haskey(results, row.station)
            sensor = results[row.station]
            # <1000 veh/lane/hr is free flow
            # data are in 5 minute bins
            if row.total_flow < sensor.lanes * 1000 / 12
                # update quantiles
                fit!(sensor.ffs, row.avg_speed_mph)
            end
        end
    end
end

function main()
    parsed_args = parse_args(ARGS, s)
    data_dir = parsed_args["data_dir"]
    all_files = readdir(data_dir)
    file_pattern = r"^d[0-9]{2}_text_station_5min_([0-9]{4})_([0-9]{2})_([0-9]{2}).txt.gz$"

    Threads.nthreads() > 1 && error("multithreading causes deadlock in dataframe combine somehow. run with one thread.")

    all_days = Set([
        period_days_for_year(2021)...,
        period_days_for_year(2020)...,
        period_days_for_year(2019)...,
        period_days_for_year(2018)...,
        period_days_for_year(2017)...,
        period_days_for_year(2016)...
    ])

    # TODO why does D12 have one more file than D04?
    candidate_files = collect(filter(all_files) do f
        mat = match(file_pattern, f)
        if isnothing(mat)
            return false
        else
            y = parse(Int64, mat[1])
            m = parse(Int64, mat[2])
            d = parse(Int64, mat[3])
            date = Dates.Date(y, m, d)
            return in(date, all_days)
        end
    end)

    results = Dict{Int64, NamedTuple{(:lanes, :ffs), Tuple{Int64, OnlineStats.Series}}}()

    # first, read meta information
    meta = CSV.read("data/sensor_meta_geo.csv", DataFrame)

    map(zip(meta.ID, meta.Lanes)) do row
        id, lanes = row
        results[id] = (lanes=lanes, ffs=Series(Counter(), Quantile([0.5, 0.75, 0.95])))
    end

    total_files = length(candidate_files)
    @info "Found $total_files candidate files"

    @showprogress for file in candidate_files
        day = KFactors.read_day_file(joinpath(data_dir, file))
        update_ffs(Tables.namedtupleiterator(day), results)
    end

    df = DataFrame(map(collect(pairs(results))) do sensor
        id, rec = sensor
        val = value(rec.ffs)

        (id=id, lanes=rec.lanes, count=val[1], median=val[2][1], pct75=val[2][2], pct95=val[2][3])
    end)

    CSV.write("data/free_flow_speeds.csv", df)
end

main()