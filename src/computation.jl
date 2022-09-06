const DEFAULT_PERMUTATIONS = 1000
const DEFAULT_MIN_COMPLETE = 0.25
const SEED = 867_5309

function join_data(data_path, meta_path)
    sensor_meta = CSV.read(meta_path, DataFrame)
    data = DataFrame(read_parquet(data_path))

    select!(sensor_meta, [:ID, :District, :urban, :Lanes, :Latitude, :Longitude])

    # add sensor metadata to peak data
    data = innerjoin(data, sensor_meta, on=:station => :ID)

    # reassemble the date and time fields
    data.date = Date.(data.year, data.month, data.day)
    #data.peak_hour_start = passmissing(Time).(data.peak_hour_start_hour, data.peak_hour_start_minute)

    # drop columns no longer needed

    #select!(data, Not([:year, :month, :day, :peak_hour_start_hour, :peak_hour_start_minute]))

    # add a period field
    #data.period = CategoricalArray(period_for_date.(data.date))

    # how many observations in each period?
    #@info "Observations in each period" combine(groupby(data, :period), nrow)

    # add the day of the week
    data.dayofweek = CategoricalArray(Dates.dayname.(data.date))

    @info "Before filtering, data has $(nrow(data)) rows"

    # filter data before returning
    # filter to just mainline (i.e. not onramps etc)
    data = data[
        # filter to weekdays
        in.(data.dayofweek, [Set(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])]) .&
        # are not holidays or adjacent
        .!in.(data.date, [HOLIDAYS_Δ1]) .&
        # and are mainline observations, i.e. not onramps etc
        (data.station_type .== "ML"),:]


    # remove columns that cannot be serialized to parquet
    select!(data, Not([:dayofweek, :date]))

    @info "After filtering, data has $(nrow(data)) rows"
    return data
end

function read_data(data_path; dropmissing = true)
    data = DataFrame(read_parquet(data_path))

    # not missing occupancy data
    data.has_missing = .!(
        coalesce.(isfinite.(data.peak_hour_occ), [false]) .&
        # and had traffic
        coalesce.(data.total_flow .> 0, [false])
    )

    # and don't have extreme occupancy
    # this does need to be done in a separate call, because we want it to be based
    # on the percentiles of the _filtered_ dataset
    max_occ = percentile(data[.!data.has_missing, :peak_hour_occ], 99)
    @info @sprintf "Removing sensors days with peak-hour occ above 99th percentile (%.2f%%)" max_occ * 100
    data.has_missing .|= (data.peak_hour_occ .> max_occ)

    if dropmissing
        data = data[.!data.has_missing, :]
    end

    # reassemble the date and time fields
    data.date = Date.(data.year, data.month, data.day)
    data.peak_hour_start = passmissing(Time).(data.peak_hour_start_hour, data.peak_hour_start_minute)

    # drop columns no longer needed

    select!(data, Not([:year, :month, :day, :peak_hour_start_hour, :peak_hour_start_minute]))

    data
end

function complete_enough_sensors(subset, period, min_complete)
    complete_by_sensor = @pipe groupby(subset, :station) |>
        # This is total not imputed or missing, because if they were missing for another reason they wouldn't be in the
        # peaks file at all, because that entire sensor-day would have been dropped during peak calculation. Since we sum
        # up just the ones that are present, and then divide by the total possible periods, we are getting a percent complete.
        # Note in paper how imputation is binarized in ingest?
        combine(_, :periods_imputed => (x -> sum(288 .- x)) => :total_not_imputed)
    # DST would be an issue here if we didn't drop Sundays from our analysis
    total_periods_possible = sum(length.([Periods.filter_days(p[1], p[2]) for p in period])) * 288
    complete_by_sensor.proportion_complete = complete_by_sensor.total_not_imputed ./ total_periods_possible
    Set(complete_by_sensor[complete_by_sensor.proportion_complete .≥ min_complete, :station])
end

# create data for permutation test
# min_complete will drop sensors that are not at least this proportion complete in both the
# pre-pandemic and post-lockdown periods
function create_test_data(data, periods; min_complete=DEFAULT_MIN_COMPLETE)
    period = Periods.period_for_date.(data.date, Ref(periods))

    test_data = data[period .∈ Ref(Set([:prepandemic, :lockdown, :postlockdown])), :]
    test_data.period = Periods.period_for_date.(test_data.date, Ref(periods))

    # figure out sensor completeness
    sensors = intersect(
        complete_enough_sensors(test_data[test_data.period .== :prepandemic, :], periods[:prepandemic], min_complete),
        complete_enough_sensors(test_data[test_data.period .== :postlockdown, :], periods[:postlockdown], min_complete),
    )

    test_data[test_data.station .∈ Ref(sensors), :]
end

function cumulative_dist(v)
    sorted = sort(v[isfinite.(v)])
    return sorted, (1:length(sorted)) ./ length(sorted)
end

const PREPANDEMIC = 1
const POSTLOCKDOWN = 2

function permute(data, n_permutations, col)
    @assert all(in.(data.period, [Set([:prepandemic, :postlockdown])])) "Permute assumes only pre-pandemic and post-lockdown data are present"

    output = zeros(Float64, (2, n_permutations))
    dates_and_periods = combine(groupby(data, :date), :period => first => :period)
    
    rng = MersenneTwister(SEED)
    
    dates = dates_and_periods.date
    periods = dates_and_periods.period
    
    int_period::Vector{Int64} = map(periods) do period
        if period == :prepandemic
            return PREPANDEMIC
        elseif period == :postlockdown
            return POSTLOCKDOWN
        end
    end
    
    period_for_day = Dict{Date, Int64}()
    for permutation in 1:n_permutations
        shuffle!(rng, int_period)
        
        for i in 1:length(dates)
            period_for_day[dates[i]] = int_period[i]
        end
       
        n = zeros(Int64, 2)
        
        # barrier function for type stability
        _permute_inner!(data.date, data[!, col], period_for_day, output, n, permutation)
        
        output[:, permutation] ./= n
    end
    
    return output
end

function _permute_inner!(date, col, period_for_day, output, n, permutation)
    for row in zip(date, col)
        period = period_for_day[row[1]]
        output[period, permutation] += row[2]
        n[period] += 1
    end
end

function permutation_test(data, col; n_permutations=DEFAULT_PERMUTATIONS)
    means = permute(data, n_permutations, col)
    sampling_dist_diff_means = means[POSTLOCKDOWN, :] .- means[PREPANDEMIC, :]
    obs_means = combine(groupby(data, :period), col => mean => :mean)
    obs_diff = obs_means[obs_means.period .== :postlockdown, :mean][1] - obs_means[obs_means.period .== :prepandemic, :mean][1]
    n_sensors = length(unique(data.station))
    
    if obs_diff <= median(sampling_dist_diff_means)
        pval = 2 * mean(sampling_dist_diff_means .< obs_diff)
    else
        pval = 2 * mean(sampling_dist_diff_means .> obs_diff)
    end
    
    return (ptest=obs_diff, pval=pval, n_sensors=n_sensors)
end

idx_for_time(time) = Dates.hour(time) * 12 + Dates.minute(time) ÷ 5 + 1
time_for_idx(idx) = Dates.Time((idx - 1) ÷ 12, (idx - 1) % 12 * 5)