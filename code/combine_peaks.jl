# after running calculate_peaks.jl, this combines them to single file

using Parquet
using ArgParse
using DataFrames
using Printf
using ProgressBars

s = ArgParseSettings()

@add_arg_table s begin
    "data_dir"
        help = "Directory with data"
    "output_file"
        help = "Output file (Parquet format)"
end

function main()
    parsed_args = parse_args(ARGS, s)
    data_dir = parsed_args["data_dir"]
    output_file = parsed_args["output_file"]
    all_files = readdir(data_dir)
    file_pattern = r"d[0-9]{2}_text_station_5min_[0-9]{4}_[0-9]{2}_[0-9]{2}_peaks.parquet$"

    candidate_files = collect(filter(f -> occursin(file_pattern, f), all_files))

    @printf "Found %d candidate files\n" length(candidate_files)

    tables = []
    sizehint!(tables, length(candidate_files))

    # this was multithreaded but threads don't save us much, this is IO bound
    pbar = ProgressBar(candidate_files)
    for file in pbar
        set_multiline_postfix(pbar, file)
        push!(tables, DataFrame(read_parquet(joinpath(data_dir, file))))
    end

    @printf "Read %d files, concatenating\n" length(tables)

    all_tables = vcat(tables...)

    write_parquet(output_file, all_tables)

    @printf "Wrote %d observations to %s\n" nrow(all_tables) output_file
end

main()