out = zeros(Int64, Threads.nthreads())

# force precompile
Threads.@threads for i in collect(1:1000)
    out[Threads.threadid()] = i
end

count = Threads.Atomic{Int64}(0)

@time Threads.@threads for i in collect(1:100_000_000)
    current_count = Threads.atomic_add!(count, 1)[]
    out[Threads.threadid()] = current_count + i
end

grand = sum(out)
println("$grand")