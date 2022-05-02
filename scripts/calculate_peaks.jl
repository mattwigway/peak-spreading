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
using KFactors

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
            return true #in(date, all_days)
        end
    end)

    total_files = length(candidate_files)
    @info "Found $total_files candidate files"

    for file in ProgressBar(candidate_files)
        parse_file(joinpath(data_dir, file))
    end
end

main(ARGS)
#parse_file("/Volumes/Pheasant Ridge/pems/d12_text_station_5min_2021_01_03.txt.gz")