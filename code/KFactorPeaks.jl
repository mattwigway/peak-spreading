module KFactorPeaks

using CSV
using Parquet
using CodecZlib
using StatsBase
using Dates
using Suppressor
using DataFrames
using Missings
using Logging
using Random

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
            with_logger(NullLogger()) do
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
    if any(ismissing.(avg_occ)) || any(ismissing.(time))
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
        # + 11 because i:i + 12 has length 13, is one hour and 5 minutes
        peak_amt = sum(sorted_occ[i:i + 11])
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

# Really this is just an entropy function, but we name it more specifically
# because we're only using it for occupancy
function occupancy_entropy(avg_occ)
    if any(ismissing.(avg_occ))
        return missing
    end
    
    tot_occ = sum(avg_occ)

    if tot_occ > 0
        norm_avg_occ = avg_occ ./ tot_occ
        # don't normalize if sum is zero, but still run entropy calc so we get nonnegative assertion
        # if we just short-circuited, entropy([-1, 1]) => 0
    else
        norm_avg_occ = avg_occ
    end

    entropy = 0

    for p in norm_avg_occ
        @assert p >= 0
        if p > 0
            entropy -= p * log2(p)
        end
    end

    return entropy
end

# How many 5-minute periods were at least partially imputed from this sensor?
# enforce Integer to make sure we avoid floating-point roundoff errors
periods_imputed(pct_obs::AbstractVector{<:Union{<:Integer, Missing}}) = any(ismissing.(pct_obs)) ? missing : sum(pct_obs .!= 100)

# What was the longest amount of time (in minutes) that data were imputed from this sensor?
function longest_imputed_time(times, pct_obs::AbstractVector{<:Union{<:Integer, Missing}})
    if any(ismissing.(times)) || any(ismissing.(pct_obs))
        return missing
    end

    sorter = sortperm(times)
    ordered_any_missing = pct_obs[sorter] .!= 100

    if !any(ordered_any_missing)
        return 0
    end

    vals, runlength = rle(ordered_any_missing)
    # the longest runlength where any_missing == true, * 5 to convert to minutes
    return max(runlength[vals]...) * 5
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
                [:time, :total_flow] => peak_hour_factor_binary => [:peak_flow_start, :peak_hour_flow],
                :avg_occ => occupancy_entropy => :occ_entropy,
                :avg_occ => sum => :total_occ,
                :total_flow => sum => :total_flow,
                :lane_type => first => :station_type,
                :freeway_number => first => :freeway_number,
                :pct_obs => periods_imputed => :periods_imputed,
                [:time, :pct_obs] => longest_imputed_time => :longest_imputed_time,
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
                peaks.peak_flow_start_hour = passmissing(Dates.hour).(peaks.peak_flow_start)
                peaks.peak_flow_start_minute = passmissing(Dates.minute).(peaks.peak_flow_start)
                peaks[!, :day_of_week] .= Dates.dayname(d.timestamp[1])

                # remove the raw peak_hour_start field as parquet cannot handle times
                select!(peaks, Not([:peak_hour_start, :peak_flow_start]))

                # write out
                write_parquet(outf * ".in_progress", peaks)

                # make sure it was completely written and not corrupted before renaming
                mv(outf * ".in_progress", outf)
            end
        end
    end
end

export parse_file

end