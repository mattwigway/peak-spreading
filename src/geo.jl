# geographic operations based on distance matrices between sensors

using Geodesy
using DataFrames
using CSV

# get a distance matrix for a slice of the overall data frame with sensors on the same road
function sub_distance_matrix(df)
    from_sensor = Vector{Int64}()
    to_sensor = Vector{Int64}()
    distance = Vector{Float64}()

    sizehint!.([from_sensor, to_sensor, distance], [nrow(df) * (nrow(df) - 1)])

    for origin in Tables.rows(df)
        frloc = LatLon(lat=origin.Latitude, lon=origin.Longitude)
        for dest in Tables.rows(df)
            if origin.ID == dest.ID
                continue
            end
            # NB this is doing twice as much work as it needs to because it calculates both a > b and b > a which are the same
            # but only fix if it's slow
            toloc = LatLon(lat=dest.Latitude, lon=dest.Longitude)
            dist = euclidean_distance(frloc, toloc)
            push!(from_sensor, origin.ID)
            push!(to_sensor, dest.ID)
            push!(distance, dist)
        end
    end

    return DataFrame(from_sensor=from_sensor, to_sensor=to_sensor, distance=distance)
end

# get a distance matrix between all sensors, in long format, sorted by from_sensor and distance
# no distances computed between sensors on different roads/directions
function get_distance_matrix(meta_path)
    meta = CSV.read(meta_path, DataFrame)
    full_dist_mat = combine(groupby(meta, [:Fwy, :Dir]), sub_distance_matrix)
    sort!(full_dist_mat, [:from_sensor, :distance])
    return full_dist_mat
end

# return a function to extract the nearest nonmissing values for each sensor
function find_nearest_values(distance_matrix, data, col)
    return (stations, date) -> begin
        station = stations[1]
        nearest_stations = @view distance_matrix[distance_matrix.from_sensor .== station, [:to_sensor, :distance]]
        @assert issorted(nearest_stations.distance)
        return map(date) do date
            sensors_for_date = @view data[(data.date .== date) .& (data.periods_imputed .== 0) .& .!ismissing.(data[:, col]), :]
            for to_sensor in nearest_stations.to_sensor
                sel = (sensors_for_date.station .== to_sensor)
                if any(sel)
                    return sensors_for_date[sel, col][1]
                end
            end
            return missing
        end
    end
end