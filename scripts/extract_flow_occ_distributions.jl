# create flow-occupancy distributions for all sensors

using KFactors, DataFrames, CSV, Tables, Dates, ArgParse, OnlineStats, ProgressBars, Random

s = ArgParseSettings()

@add_arg_table s begin
    "data_dir"
        help = "Directory with data"
end

function _update(itr, period, results)
    for row in itr
        if !ismissing(row.total_flow) && !ismissing(row.avg_occ) && haskey(results, row.station)
            st = results[row.station]
            tp = KFactors.idx_for_time(row.time)
            fit!(st[period].occ[tp], row.avg_occ)
            fit!(st[period].flow[tp], row.total_flow)
        end
    end
end

function handle(file, results)
    data = KFactors.read_day_file(file)
    # barrier function for perf
    period = Symbol(KFactors.Periods.period_for_date(Dates.Date(data.timestamp[1]), KFactors.Periods.SPRING_2022))
    _update(Tables.namedtupleiterator(data), period, results)
end

function main()
    parsed_args = parse_args(ARGS, s)
    data_dir = parsed_args["data_dir"]
    all_files = readdir(data_dir)
    file_pattern = r"^d[0-9]{2}_text_station_5min_([0-9]{4})_([0-9]{2})_([0-9]{2}).txt.gz$"

    Threads.nthreads() > 1 && error("multithreading causes deadlock in dataframe combine somehow. run with one thread.")

    all_days = Set([
        KFactors.Periods.period_days_for_year(2022, KFactors.Periods.spring_2022_period)...,
        KFactors.Periods.period_days_for_year(2019, KFactors.Periods.spring_2022_period)...,
        KFactors.Periods.period_days_for_year(2018, KFactors.Periods.spring_2022_period)...,
        KFactors.Periods.period_days_for_year(2017, KFactors.Periods.spring_2022_period)...,
        KFactors.Periods.period_days_for_year(2016, KFactors.Periods.spring_2022_period)...
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

    # make progress bar more accurate
    shuffle!(candidate_files)

    total_files = length(candidate_files)
    @info "Found $total_files candidate files"

    meta = CSV.read(joinpath(Base.source_dir(), "../data/sensor_meta_geo.csv"), DataFrame)


    RecType = @NamedTuple{flow::Vector{Mean}, occ::Vector{Mean}}
    results = Dict{Int64, @NamedTuple{prepandemic::RecType, lockdown::RecType, postlockdown::RecType}}()

    genrec() = (
        flow = map(_ -> Mean(Float64), 1:(24 * 12)),
        occ = map(_ -> Mean(Float64), 1:(24 * 12))
    )

    for id in meta.ID
        results[id] = (
            prepandemic=genrec(),
            lockdown=genrec(),
            postlockdown=genrec()
        )
    end

    for file in ProgressBar(candidate_files)
        handle(joinpath(data_dir, file), results)
    end

    RowType = @NamedTuple{station::Int64, period::Symbol, time::Time, flow::Float64, occ::Float64, flow_obs::Int64, occ_obs::Int64}
    output = Vector{RowType}()

    for sensor in pairs(results)
        id, recs = sensor
        for period in (:prepandemic, :lockdown, :postlockdown)
            for tp in 1:288
                push!(output, (
                    station=id,
                    period=period,
                    time=KFactors.time_for_idx(tp),
                    flow=value(recs[period].flow[tp]),
                    occ=value(recs[period].occ[tp]),
                    flow_obs=nobs(recs[period].flow[tp]),
                    occ_obs=nobs(recs[period].occ[tp]),
                ))
            end
        end
    end

    CSV.write(joinpath(Base.source_dir(), "../data/flow_occ_distr.csv"), output)
end

main()
