# Test the Bureau of Public Roads volume-delay functions

using KFactors

@testset "Volume Delay Functions inverse, freeflow speed $ff_speed mph" for ff_speed in 30:1.0:70
    # ensure one is the inverse of the other
    speeds = 1:1.0:ff_speed
    @test all(VDF.bpr_demand_to_speed.(ff_speed, VDF.bpr_speed_to_demand.(ff_speed, speeds)) .≈ speeds)
end

@testset "Volume Delay Function domain" begin
    @test_throws ErrorException VDF.bpr_speed_to_demand(70, 80)
end