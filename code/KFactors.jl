# Factor out common code for computing K-factors
# use in a notebook by doing include("path/to/KFactors.jl"); using .KFactors
# Author: Matt Bhagat-Conway <mwbc@unc.edu> and Sam Zhang, 2021-09-17

module KFactors

using Dates
using CSV
using Parquet
using DataFrames
using Logging
using StatsBase
using Statistics
using CategoricalArrays
using Random
using Printf

include("holidays.jl")
include("geo.jl")


const DEFAULT_PERMUTATIONS = 1000

const SEED = 867_5309

# Get the relevant period for a particular year - from the third Tuesday of June to 32 days later
# dates where there was no mask mandate in California
# statewide mask mandate lifted June 15: https://www.latimes.com/science/story/2021-07-27/timeline-cdc-mask-guidance-during-covid-19-pandemic
# LA county requires masks again after July 17: https://www.latimes.com/california/story/2021-07-15/l-a-county-will-require-masks-indoors-amid-covid-19-surge
function period_for_year(year)
    start = Dates.tonext(d -> Dates.dayofweek(d) == Dates.Tuesday, Date(year, 5, 31)) + Dates.Week(2)
    endd = start + Dates.Day(32)
    return start, endd
end

function period_days_for_year(year)
    start, endd = period_for_year(year)
    days = filter(start:Dates.Day(1):endd) do d
        Dates.dayofweek(d) != Dates.Saturday &&
        Dates.dayofweek(d) != Dates.Sunday &&
        !in(d, HOLIDAYS_Δ1)
    end

    return collect(days)
end

const PERIODS = Dict(
    "postpandemic" => [period_for_year(2021)],
    "pandemic" => [period_for_year(2020)],
    "prepandemic" => [
        period_for_year(2019),
        period_for_year(2018),
        period_for_year(2017),
        period_for_year(2016)
    ]
)

function period_for_date(date)
    for (name, dates) in PERIODS
        for range in dates
            if date >= range[1] && date <= range[2]
                return name
            end
        end
    end
    return missing
end

# TODO this function is painfully slow. Why? The join? Should we cache the join?
function read_data(data_path, meta_path; dropmissing=true)
    sensor_meta = CSV.read(meta_path, DataFrame)
    data = DataFrame(read_parquet(data_path))

    select!(sensor_meta, [:ID, :District, :urban, :Lanes, :Latitude, :Longitude])

    # add sensor metadata to peak data
    data = innerjoin(data, sensor_meta, on=:station => :ID)

    # reassemble the date and time fields
    data.date = Date.(data.year, data.month, data.day)
    data.peak_hour_start = passmissing(Time).(data.peak_hour_start_hour, data.peak_hour_start_minute)
    data.peak_flow_start = passmissing(Time).(data.peak_flow_start_hour, data.peak_flow_start_minute)

    # drop columns no longer needed
    select!(data, Not([:year, :month, :day, :peak_hour_start_hour, :peak_hour_start_minute,
        :peak_flow_start_hour, :peak_flow_start_minute]))

    # add a period field
    data.period = CategoricalArray(period_for_date.(data.date))

    # how many observations in each period?
    @info "Observations in each period" combine(groupby(data, :period), nrow)

    # add the day of the week
    data.dayofweek = CategoricalArray(Dates.dayname.(data.date))

    # add an indicator to indicate a sensor was present in all periods
    transform!(groupby(data, :station), :period => (v ->
        any(coalesce.(v .== "pandemic", [false])) &&
        any(coalesce.(v .== "prepandemic", [false])) &&
        any(coalesce.(v .== "postpandemic", [false]))) => :present_in_all_periods)

    @info "Before filtering, data has $(nrow(data)) rows"

    # filter data before returning
    # filter to just mainline (i.e. not onramps etc)
    data = data[
        # filter to weekdays
        in.(data.dayofweek, [Set(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])]) .&
        # that are in one of the defined periods
        .!ismissing.(data.period) .&
        # are not holidays or adjacent
        .!in.(data.date, [HOLIDAYS_Δ1]) .&
        # and had observations in all periods
        data.present_in_all_periods .&
        # and are mainline or conventional highway observations, i.e. not onramps etc
        in.(data.station_type, [Set(["ML", "CH"])]), :]


    if dropmissing
        # not missing occupancy data
        data = data[
            coalesce.(isfinite.(data.peak_hour_occ), [false]) .&
            # and had traffic
            coalesce.(data.total_flow .> 0, [false]), :]

        # and don't have extreme occupancy
        # this does need to be done in a separate call, because we want it to be based
        # on the percentiles of the _filtered_ dataset
        max_occ = percentile(data.peak_hour_occ, 99)
        @info @sprintf "Removing sensors days with peak-hour occ above 99th percentile (%.2f%%)" max_occ
        data = data[data.peak_hour_occ .≤ max_occ, :]
    end

    @info "After filtering, data has $(nrow(data)) rows"
    return data
end

function cumulative_dist(v)
    sorted = sort(v[isfinite.(v)])
    return sorted, (1:length(sorted)) ./ length(sorted)
end

function cumulative_dist(v, w)
    f = isfinite.(v) .& isfinite.(w)
    v = v[f]
    w = w[f]
    sorter = sortperm(v)
    sorted = v[sorter]
    return sorted, cumsum(w[sorter]) / sum(w)
end

const PREPANDEMIC = 1
const POSTPANDEMIC = 2

function permute(data, n_permutations, col)
    @assert all(in.(data.period, [Set(["prepandemic", "postpandemic"])])) "Permute assumes only pre and post-pandemic data are present"

    output = zeros(Float64, (2, n_permutations))
    dates_and_periods = combine(groupby(data, :date), :period => first => :period)
    
    rng = MersenneTwister(SEED)
    
    dates = dates_and_periods.date
    periods = dates_and_periods.period
    
    int_period::Vector{Int64} = map(periods) do period
        if period == "prepandemic"
            return PREPANDEMIC
        elseif period == "postpandemic"
            return POSTPANDEMIC
        end
    end
    
    period_for_day = Dict{Date, Int64}()
    for permutation in 1:n_permutations
        shuffle!(rng, int_period)
        
        for i in 1:length(dates)
            period_for_day[dates[i]] = int_period[i]
        end
       
        n = zeros(Int64, 2)
        
        for row in zip(data.date::Vector{Date}, data[!, col]::Vector{Union{Missing, Float64}})
            period = period_for_day[row[1]]
            output[period, permutation] += row[2]
            n[period] += 1
        end
        
        output[:, permutation] ./= n
    end
    
    return output
end

function permutation_test(data, col; n_permutations=DEFAULT_PERMUTATIONS)
    means = permute(data, n_permutations, col)
    sampling_dist_diff_means = means[POSTPANDEMIC, :] .- means[PREPANDEMIC, :]
    obs_means = combine(groupby(data, :period), col => mean => :mean)
    obs_diff = obs_means[obs_means.period .== "postpandemic", :mean][1] - obs_means[obs_means.period .== "prepandemic", :mean][1]
    n_sensors = length(unique(data.station))
    
    if obs_diff <= mean(sampling_dist_diff_means)
        pval = 2 * mean(sampling_dist_diff_means .< obs_diff)
    else
        pval = 2 * mean(sampling_dist_diff_means .> obs_diff)
    end
    
    return (ptest=obs_diff, pval=pval, n_sensors=n_sensors)
end

export HOLIDAYS, HOLIDAYS_Δ1, read_data, permutation_test, period_for_year, period_days_for_year
end