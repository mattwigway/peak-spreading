#=
Create fundamental diagrams using PeMS 5-minute data
=#

using ArgParse, GLMakie, KFactors, Logging, DataFrames, ProgressMeter, CSV, Colors, Random

const CAROLINA_BLUE = colorant"#4B9CD3"
const RESOLUTION = (1600, 2200)

const ARGTABLE = ArgParseSettings()
@add_arg_table! ARGTABLE begin
    "output_file"
        help = "File to save fundamental diagrams to"
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

    res_channel = Channel{Union{DataFrame, Nothing}}()

    # start graph generation thread
    figtask = Threads.@spawn assemble_graph(res_channel, Progress(length(files)))

    # start reader thread
    tasks = Vector{Task}()
    sizehint!(tasks, length(files))
    Threads.@spawn begin
        for file in files
            # Files read sequentially, avoid disk thrash
            day = KFactors.read_day_file(file)
            task = Threads.@spawn begin
                proc = process_file($day, geo)
                put!(res_channel, proc)
            end
            push!(tasks, task)
        end
        wait.(tasks)
        put!(res_channel, nothing)  # signal that we're done, after all tasks completed
    end

    figure = fetch(figtask)
    # @info figure.resolution
    save(args["output_file"], figure, resolution=RESOLUTION)
    figure
end    

function process_file(day, geo)
    day = innerjoin(day, geo, on=:station=>:ID)

    day.per_lane_flow_vph = day.total_flow ./ day.Lanes .* 12; # 12 5-minute periods per hour, convert to veh/hour

    # discard data with per-lane flow greater than 2400
    day = day[(coalesce.(day.per_lane_flow_vph .≤ 2400, false)
        .&& .!ismissing.(day.avg_speed_mph)
        .&& .!ismissing.(day.total_flow)
        .&& .!ismissing.(day.avg_occ)), :]

    day.vehdens = day.total_flow ./ (day.avg_speed_mph ./ 12) ./ day.Lanes;

    return day
end

function assemble_graph(channel, p)
    @info "assembly running"
    Theme(fontsize=40) |> set_theme!

    fig = Figure(resolution=RESOLUTION)

    density_speed = Axis(fig[1, 1], xlabel="Density (veh/mile)", ylabel="Speed (mph)")
    speed_flow = Axis(fig[2, 1], xlabel="Speed (mph)", ylabel="Veh/lane/hour")
    density_flow = Axis(fig[3, 1], xlabel="Density (veh/mile)", ylabel="Veh/lane/hour")

    while true
        day = take!(channel)
        isnothing(day) && break

        scatter!(density_speed, day.vehdens, day.avg_speed_mph, markersize=0.01, alpha=0.75, color=CAROLINA_BLUE)
        scatter!(speed_flow, day.avg_speed_mph, day.per_lane_flow_vph, markersize=0.01, alpha=0.75, color=CAROLINA_BLUE)
        scatter!(density_flow, day.vehdens, day.per_lane_flow_vph, markersize=0.01, alpha=0.75, color=CAROLINA_BLUE)

        next!(p)
    end

    # NB not exactly right, will finish! before progress bar completely done due to empty day files
    finish!(p)

    fig
end

FIG = main(ARGS)