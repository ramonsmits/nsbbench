qa := "bench.a"
qb := "bench.b"
mqbench := "dotnet run -c Release --project src/mqbench --"
nsbbench := "dotnet run -c Release --project src/nsbbench --"

# start container, build, run benchmarks
run: up build bench

build:
    dotnet build src/nsbbench.slnx -c Release -v quiet

up:
    podman-compose up -d
    @echo "Waiting for IBM MQ..."
    @until podman-compose exec ibmmq dspmq 2>/dev/null | grep -qi RUNNING; do sleep 2; done
    @echo "IBM MQ ready"

down:
    podman-compose down

[private]
drain:
    -@{{mqbench}} receive {{qa}} none express -n 1000000 2>/dev/null
    -@{{mqbench}} receive {{qb}} none express -n 1000000 2>/dev/null

bench: drain
    {{nsbbench}} benchmark
