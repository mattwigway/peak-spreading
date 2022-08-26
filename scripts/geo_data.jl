using KFactors, Parquet, Logging

function main()
    data = KFactors.join_data("data/all_district_peaks.parquet", "data/sensor_meta_geo.csv")
    @info "read data"
    write_parquet("data/peaks_merged.parquet", data)
end

main()