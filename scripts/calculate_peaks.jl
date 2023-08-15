# Calculate peaks for all sensor data

using Distributed
addprocs(exeflags="--project=$(Base.active_project())")

@everywhere begin
    using CSV, Parquet, CodecZlib, ArgParse, StatsBase, Dates, Printf, ProgressMeter, Suppressor, DataFrames,
        Missings, Logging, Random, KFactors
end

s = ArgParseSettings()

@add_arg_table s begin
    "data_dir"
        help = "Directory with data"
end

function main(args)
    parsed_args = parse_args(args, s)
    data_dir = parsed_args["data_dir"]
    all_files = readdir(data_dir)
    file_pattern = r"^d[0-9]{2}_text_station_5min_([0-9]{4})_([0-9]{2})_([0-9]{2}).txt.gz$"

    Threads.nthreads() > 1 && error("multithreading causes deadlock in dataframe combine somehow. run with one thread.")

    # TODO why does D12 have one more file than D04?
    candidate_files = collect(filter(all_files) do f
        mat = match(file_pattern, f)
        return !isnothing(mat)
    end)

    total_files = length(candidate_files)
    @info "Found $total_files candidate files"

    # no progress meter on this, see https://github.com/timholy/ProgressMeter.jl/issues/242
    @sync @distributed for file in candidate_files
        parse_file(joinpath(data_dir, file))
    end
end

main(ARGS)

#sleep(30)  # hack to give distributed time to shut down

#parse_file("/Volumes/Pheasant Ridge/pems/d12_text_station_5min_2021_01_03.txt.gz")