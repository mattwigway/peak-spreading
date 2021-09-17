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

include("holidays.jl")

# dates where there was no mask mandate in California
# statewide mask mandate lifted June 15: https://www.latimes.com/science/story/2021-07-27/timeline-cdc-mask-guidance-during-covid-19-pandemic
# LA county requires masks again after July 17: https://www.latimes.com/california/story/2021-07-15/l-a-county-will-require-masks-indoors-amid-covid-19-surge
const DEFAULT_POST_PANDEMIC_PERIOD = [Date(2021, 6, 15), Date(2021, 7, 17)]

const DEFAULT_PERMUTATIONS = 1000

const SEED = 867_5309

function read_data(data_path, meta_path; period=DEFAULT_POST_PANDEMIC_PERIOD)
    sensor_meta = CSV.read(meta_path, DataFrame)
    data = DataFrame(read_parquet(data_path))

    # add sensor metadata to peak data
    data = leftjoin(data, sensor_meta[:, [:ID, :District, :urban]], on=:station => :ID)

    # reassemble the date and time fields
    data.date = Date.(data.year, data.month, data.day)
    data.peak_hour_start = passmissing(Time).(data.peak_hour_start_hour, data.peak_hour_start_minute)
    # drop columns no longer needed
    select!(data, Not([:year, :month, :day, :peak_hour_start_hour, :peak_hour_start_minute]))

    # add a period field
    periods = Dict(
        "postpandemic" => [period],
        "pandemic" => [period .- Dates.Year(1)],
        "prepandemic" => [
            period .- Dates.Year(2),
            period .- Dates.Year(3),
            period .- Dates.Year(4),
            period .- Dates.Year(5)
        ]
    )

    function period_for_date(date)
        for (name, dates) in periods
            for range in dates
                if date >= range[1] && date <= range[2]
                    return name
                end
            end
        end
        return missing
    end

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
        # not missing occupancy data
        coalesce.(isfinite.(data.peak_hour_occ), [false]) .&
        # are not holidays or adjacent
        .!in.(data.date, [HOLIDAYS_Δ1]) .&
        # and had observations in all periods
        data.present_in_all_periods .&
        # and are mainline observations, i.e. not onramps etc
        (data.station_type .== "ML"), :]

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

function permute!(data, n_permutations)
    @assert all(in.(data.period, [Set(["prepandemic", "postpandemic"])])) "Permute assumes only pre and post-pandemic data are present"

    output = zeros(Float64, (2, n_permutations))
    dates_and_periods = combine(groupby(data, :date), :period => first => :period)
    
    rng = MersenneTwister(SEED)
    
    period_map = Dict()
    sizehint!(period_map, nrow(dates_and_periods))
    
    for permutation in 1:n_permutations
        shuffle!(rng, dates_and_periods.period)

        for i in 1:nrow(dates_and_periods)
            period_map[dates_and_periods[i, :date]] = dates_and_periods[i, :period]
        end

        data.permuted_period = map(d -> period_map[d], data.date)

        means = combine(groupby(data, :permuted_period), :peak_hour_occ => mean => :mean_peak_occ)

        @assert nrow(means) == 2

        output[1, permutation] = means[means.permuted_period .== "prepandemic", :mean_peak_occ][1]
        output[2, permutation] = means[means.permuted_period .== "postpandemic", :mean_peak_occ][1]
    end
    
    return output
end

function permutation_test(data; n_permutations=DEFAULT_PERMUTATIONS)
    means = permute!(data, n_permutations)
    sampling_dist_diff_means = means[2, :] .- means[1, :]
    obs_means = combine(groupby(data, :period), :peak_hour_occ => mean)
    obs_diff = obs_means[obs_means.period .== "postpandemic", :peak_hour_occ_mean][1] - obs_means[obs_means.period .== "prepandemic", :peak_hour_occ_mean][1]
    n_sensors = length(unique(data.station))
    
    if obs_diff <= mean(sampling_dist_diff_means)
        pval = 2 * mean(sampling_dist_diff_means .< obs_diff)
    else
        pval = 2 * mean(sampling_dist_diff_means .> obs_diff)
    end
    
    return (ptest=obs_diff, pval=pval, n_sensors=n_sensors)
end

export HOLIDAYS, HOLIDAYS_Δ1, read_data, permutation_test
end