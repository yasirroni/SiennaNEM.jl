# SiennaNEM

[![Build Status](https://github.com/yasirroni/SiennaNEM.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/yasirroni/SiennaNEM.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/yasirroni/SiennaNEM.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/yasirroni/SiennaNEM.jl)

## Usage

See minimum working example workflow in `examples/`.

## Development

### Add data

To add data, clone the `ARPST-UniMelb/NEM-reliability-suite`

```sh
git clone git@github.com:ARPST-UniMelb/NEM-reliability-suite.git
```

If you already clone it, pull to update to the latest data

```sh
cd NEM-reliability-suite
git pull
git lfs pull
cd ..
mkdir -p data
cp -r NEM-reliability-suite/data/ data/nem12
```

<!-- 
To delete `data/nem12`,

```sh
rm -rf data/nem12
```
-->

### Installation

In your Julia global environment (starting Julia with `julia`, no `--project`),

```julia
using Pkg
Pkg.add("Revise")
Pkg.add("TestEnv")
using Revise
```

Install [`PISP.jl`](https://github.com/ARPST-UniMelb/PISP.jl#),

```julia
using Pkg
Pkg.develop(path="../PISP.jl")
```

### Start

```julia
using Pkg
Pkg.activate(".")
Pkg.instantiate()
```

### Test

> [!NOTE]  
> Tests require the `data/nem12` directory, which is currently not released.

```julia
using Pkg

Pkg.activate(".")
Pkg.resolve()
Pkg.instantiate()
Pkg.precompile()
Pkg.test()
```

### Benchmark

```julia
using Pkg
using TestEnv
TestEnv.activate("SiennaNEM")
Pkg.resolve()
```

run `bench/run_bench_data_format.jl` and `bench/run_bench_horizon.jl`
