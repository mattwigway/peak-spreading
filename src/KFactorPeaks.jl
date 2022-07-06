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

function peak_hour_factor_binary(time, avg_occ, flow)
    if any(isnodata.(avg_occ)) || any(isnodata.(time)) || any(isnodata.(flow))
        return (peak_hour_start=missing, peak_hour_occ=missing, peak_hour_occ_avg=missing, peak_hour_flow=missing)
    end
    
    sorter = sortperm(time)
    sorted_occ = avg_occ[sorter]
    sorted_time = time[sorter]
    sorted_flow = flow[sorter]  
    if length(sorted_occ) != (24 * 12)  # 12 5 minute periods per hour, 24 hours per day
        return (peak_hour_start=missing, peak_hour_occ=missing, peak_hour_occ_avg=missing, peak_hour_flow=missing)
    end
            
    highest_peak_occ = -1.0
    highest_peak_flow = -1.0
    start_of_highest_peak = missing
    for i in 1:(23 * 12)
        # + 11 because i:i + 12 has length 13, is one hour and 5 minutes
        peak_amt = sum(sorted_occ[i:i + 11])
        if peak_amt > highest_peak_occ
            highest_peak_occ = peak_amt
            highest_peak_flow = sum(sorted_flow[i:i+11])
            start_of_highest_peak = sorted_time[i]
        end
    end
    
    @assert !isnodata(start_of_highest_peak)
    
    # normalize to total traffic for day
    peak_occ_avg = highest_peak_occ / 12
    highest_peak_occ /= sum(avg_occ)
    highest_peak_flow /= sum(flow)
    return (peak_hour_start=start_of_highest_peak, peak_hour_occ=highest_peak_occ, peak_hour_occ_avg=peak_occ_avg, peak_hour_flow=highest_peak_flow)
end

# Daytime: 5am to 8pm
function is_in_daytime(five_minute_time_of_day)
    start_of_daytime = Time(5, 0)
    end_of_daytime = Time(20, 0)
    return (five_minute_time_of_day >= start_of_daytime) && (five_minute_time_of_day < end_of_daytime)
end

# Filter the avg_occ data down to daytime hours
# And return its entropy
function occupancy_entropy_daytime(time, avg_occ)
    daytime_occ = avg_occ[is_in_daytime.(time)]

    return occupancy_entropy(daytime_occ)
end

# Really this is just an entropy function, but we name it more specifically
# because we're only using it for occupancy
function occupancy_entropy(avg_occ)
    if any(isnodata.(avg_occ))
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
periods_imputed(pct_obs::AbstractVector{<:Union{<:Integer, Missing}}) = any(isnodata.(pct_obs)) ? missing : sum(pct_obs .!= 100)

# What was the longest amount of time (in minutes) that data were imputed from this sensor?
function longest_imputed_time(times, pct_obs::AbstractVector{<:Union{<:Integer, Missing}})
    if any(isnodata.(times)) || any(isnodata.(pct_obs))
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
                [:time, :avg_occ, :total_flow] => peak_hour_factor_binary => [:peak_hour_start, :peak_hour_occ, :peak_hour_occ_avg, :peak_hour_flow],
                :avg_occ => occupancy_entropy => :occ_entropy,
                [:time, :avg_occ] => occupancy_entropy_daytime => :occ_entropy_daytime,
                :avg_occ => sum => :total_occ,
                :avg_speed_mph => (speed -> sum(speed .< 50) / 12) => :hours_of_congestion,
                :total_flow => sum => :total_flow,
                :lane_type => first => :station_type,
                :freeway_number => first => :freeway_number,
                :pct_obs => periods_imputed => :periods_imputed,
                [:time, :pct_obs] => longest_imputed_time => :longest_imputed_time,
                :direction => first => :direction
            )

            if all(isnodata.(peaks.peak_hour_occ))
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
                select!(peaks, Not([:peak_hour_start]))

                # convert pooledarrays to bona fide strings
                peaks.direction = string.(peaks.direction)
                peaks.station_type = string.(peaks.station_type)

                all_missing_cols = [c for c in names(peaks) if all(isnodata.(peaks[!, c]))]
                if length(all_missing_cols) > 0
                    # TODO why?
                    @warn "File $file, some cols entirely missing in output" all_missing_cols
                    select!(peaks, Not(all_missing_cols))
                end

                # write out
                write_parquet(outf * ".in_progress", peaks)

                # make sure it was completely written and not corrupted before renaming
                mv(outf * ".in_progress", outf)
            end
        end
    end
end

isnodata(x) = ismissing(x) || isnothing(x) || (x isa Number && isnan(x))