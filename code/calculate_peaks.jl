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

s = ArgParseSettings()

@add_arg_table s begin
    "data_dir"
        help = "Directory with data"
end

# periods is a dict of period name (e.g. pre-pandemic, post-pandemic, etc.) to dates (inclusive) for said period
# days is the days of the week to retain (e.g. dropping weekends)
function read_day_file(path::String)
    cols = [:timestamp, :station, :district, :freeway_number, :direction, :lane_type, :station_len, :samples, :pct_obs, :total_flow, :avg_occ, :avg_speed_mph]
    
    try
        open(GzipDecompressorStream, path) do stream
            local data
            # suppress warnings because per-lane information is in remaining columns, and since roads
            # do not all have the same number of lanes, not all rows have same number of columns. Ignore
            # warnings about that.
            @suppress begin
                data = CSV.read(stream, DataFrame, select=collect(1:12), header=cols, dateformat="mm/dd/yyyy HH:MM:SS")#, types=types)
            end
            
            # create bare date/time fields
            data.time = Dates.Time.(data.timestamp)
            return data
        end
    catch e
        @error "File could not be read" e
        return nothing
    end
end

function peak_hour_factor_binary(time, avg_occ)
    if any(ismissing.(avg_occ))
        return (peak_hour_start=missing, peak_hour_occ=missing)
    end
    
    sorter = sortperm(time)
    sorted_occ = avg_occ[sorter]
    sorted_time = time[sorter]    
    if length(sorted_occ) != (24 * 12)  # 12 5 minute periods per hour, 24 hours per day
        return (peak_hour_start=missing, peak_hour_occ=missing)
    end
            
    highest_peak = -1.0
    start_of_highest_peak = missing
    for i in 1:(23 * 12)
        peak_amt = sum(sorted_occ[i:i + 12])
        if peak_amt > highest_peak
            highest_peak = peak_amt
            start_of_highest_peak = sorted_time[i]
        end
    end
    
    @assert !ismissing(start_of_highest_peak)
    
    # normalize to total traffic for day
    phf = highest_peak / sum(avg_occ)
    return (peak_hour_start=start_of_highest_peak, peak_hour_occ=phf)
end

function parse_file(file)
    outf = file[1:length(file) - 7] * "_peaks.parquet"

    #println(outf)

    if !isfile(outf)
        d = read_day_file(file)

        if isnothing(d)
            @error "Failed to read $file"
        else

            peaks = combine(
                groupby(d, :station),
                [:time, :avg_occ] => peak_hour_factor_binary => [:peak_hour_start, :peak_hour_occ],
                :avg_occ => sum => :total_occ,
                :total_flow => sum => :total_flow,
                :lane_type => first => :station_type,
                :freeway_number => first => :freeway_number,
                :direction => first => :direction
            )

            if all(ismissing.(peaks.peak_hour_occ))
                @warn "$(file) has no observations"
            else
                # same for all observations
                peaks[!, :year] .= Dates.year(d.timestamp[1])
                peaks[!, :month] .= Dates.month(d.timestamp[1])
                peaks[!, :day] .= Dates.day(d.timestamp[1])
                peaks.peak_hour_start_hour = passmissing(Dates.hour).(peaks.peak_hour_start)
                peaks.peak_hour_start_minute = passmissing(Dates.minute).(peaks.peak_hour_start)
                peaks[!, :day_of_week] .= Dates.dayname(d.timestamp[1])

                # remove the raw peak_hour_start field as parquet cannot handle times
                select!(peaks, Not(:peak_hour_start))

                # write out
                write_parquet(outf * ".in_progress", peaks)

                # make sure it was completely written and not corrupted before renaming
                mv(outf * ".in_progress", outf)
            end
        end
    end
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