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

@testset "Capacity" begin
    # higher capacity should lead to higher speed when demand equal
    @test VDF.bpr_demand_to_speed(70, 2400, capacity=2400) < VDF.bpr_demand_to_speed(70, 2400, capacity=4800)
    # more demand needed to get to 30 mph when capacities higher
    @test VDF.bpr_speed_to_demand(70, 30, capacity=2400) < VDF.bpr_speed_to_demand(70, 30, capacity=4800)
end

@testset "Speed flow" begin
    # uncongested flow should return flow
    @test VDF.bpr_speed_flow_to_demand(70, 60, 4600, 4800) == 4600
    # congested flow should return estimated demand
    @test VDF.bpr_speed_flow_to_demand(70, 40, 4600, 4800) > 4600
end