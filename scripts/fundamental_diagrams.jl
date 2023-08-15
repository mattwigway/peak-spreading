#=
Create fundamental diagrams using PeMS 5-minute data
=#

using ArgParse, KFactors, Logging, DataFrames, ProgressMeter, CSV, Random, RawArray

const RESOLUTION = (1600, 2200)
const MAX_SPEED_MPH = 100
const MAX_FLOW_XVPH = 300
# bumper-to-bumper 20 foot cars
const MAX_DENSITY_VPM = ceil(Int64, 5280 / 20)
const MAX_OCC_THOUSANDTHS = 220

const ARGTABLE = ArgParseSettings()
@add_arg_table! ARGTABLE begin
    "output_file"
        help = "File to save fundamental summary to"
        required = true
        arg_type = String
    "files"
        help = "Files to use for making plot"
        nargs = '+'
        arg_type = String
end

function main(args)
    args = parse_args(args, ARGTABLE)

    if length(args["files"]) == 1 && isdir(args["files"][1])
        dir = args["files"][1]
        @info "$dir is a directory, reading all .txt.gz files within"
        file_pattern = r"^d[0-9]{2}_text_station_5min_([0-9]{4})_([0-9]{2})_([0-9]{2}).txt.gz$"
        files = joinpath.(dir, filter(x -> !(match(file_pattern, x) |> isnothing), readdir(dir)))
    else
        files = args["files"]
    end

    # make progress bar more accurate by randomly sorting files
    files = shuffle(files)

    @info "Parsing $(length(files)) files"

    geo = CSV.read(joinpath(Base.source_dir(), "../data/sensor_meta_geo.csv"), DataFrame)

    result = zeros(Int64, (MAX_SPEED_MPH, MAX_FLOW_XVPH, MAX_DENSITY_VPM, MAX_OCC_THOUSANDTHS))

    # start reader thread
    tasks = Vector{Task}()
    sizehint!(tasks, length(files))
    @showprogress for file in files
        # Files read sequentially, avoid disk thrash
        day = nothing
        for i in 1:3
            day = KFactors.read_day_file(file)
            isnothing(day) || break
        end

        if isnothing(day)
            @warn "$file could not be read after 2 retries"
            continue
        end

        # NB it is expected that file processing is faster than disk reads. If this is not the
        # case, memory footprint will expand unbounded, and would need to add something to this loop to prevent
        # too many files in memory at once.
        process_file(day, geo, result)
    end

    # sum up across threads
    #result = dropdims(sum(result; dims=1); dims=1)
    # compress uses variable width (zigzag) integer encoding, which will help a lot here
    rawrite(result, args["output_file"]; compress=true)
end    

function process_file(day, geo, result)
    day = innerjoin(day, geo, on=:station=>:ID)

    day.per_lane_flow_vph = day.total_flow ./ day.Lanes .* 12; # 12 5-minute periods per hour, convert to veh/hour

    # discard data with per-lane flow greater than 2400
    day = day[(coalesce.(day.per_lane_flow_vph .≤ 2400, false)
        .&& .!ismissing.(day.avg_speed_mph)
        .&& .!ismissing.(day.total_flow)
        .&& .!ismissing.(day.avg_occ)
        .&& (day.pct_obs .== 100)), :]

    # (veh/5 min) / (miles / 5 min) = (veh / mile)
    day.vehdens = day.total_flow ./ (day.avg_speed_mph ./ 12) ./ day.Lanes;

    for row in Tables.namedtupleiterator(day)
        process_row(row, result)
    end
end

function process_row(row, result)
    if row.avg_speed_mph ≥ 1 && row.avg_speed_mph < (MAX_SPEED_MPH + 1) &&
        row.vehdens ≥ 1 && row.vehdens < (MAX_DENSITY_VPM + 1) &&
        row.per_lane_flow_vph ≥ 10 && row.per_lane_flow_vph / 10 < (MAX_FLOW_XVPH + 1) &&
        row.avg_occ ≥ 1e-3 && row.avg_occ * 1000 < (MAX_OCC_THOUSANDTHS + 1)
        result[floor(Int64, row.avg_speed_mph), floor(Int64, row.per_lane_flow_vph / 10),
            floor(Int64, row.vehdens), floor(Int64, row.avg_occ * 1000)] += 1
    end
end

main(ARGS)