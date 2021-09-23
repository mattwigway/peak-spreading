# Calculate peaks for all sensor data

using CSV
using Parquet
using CodecZlib
using ArgParse
using StatsBase
using Dates
using Printf
using ProgressBars
using Suppressor
using DataFrames
using Missings
using Logging
using Random

include("./KFactorPeaks.jl")
using .KFactorPeaks

s = ArgParseSettings()

@add_arg_table s begin
    "data_dir"
        help = "Directory with data"
end

function main()
    parsed_args = parse_args(ARGS, s)
    data_dir = parsed_args["data_dir"]
    all_files = readdir(data_dir)
    file_pattern = r"d[0-9]{2}_text_station_5min_[0-9]{4}_[0-9]{2}_[0-9]{2}.txt.gz"

    # TODO why does D12 have one more file than D04?
    candidate_files = collect(filter(f -> occursin(file_pattern, f), all_files))

    total_files = length(candidate_files)
    @info "Found $total_files candidate files"

    for (idx, file) in enumerate(candidate_files)
        if idx % 25 == 0
            @info @sprintf "%d / %d files (%.1f%%) complete (%s)" idx total_files idx / total_files * 100 file
        end
        parse_file(joinpath(data_dir, file))
    end
end

main()
#parse_file("/Volumes/Pheasant Ridge/pems/d12_text_station_5min_2021_01_03.txt.gz")