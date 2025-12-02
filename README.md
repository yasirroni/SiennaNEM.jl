# SiennaNEM

[![Build Status](https://github.com/yasirroni/SiennaNEM.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/yasirroni/SiennaNEM.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/yasirroni/SiennaNEM.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/yasirroni/SiennaNEM.jl)

SiennaNEM.jl is a framework designed to enable efficient operational scheduling of the NEM. It builds upon Sienna, formerly known as Scalable Integrated Infrastructure Planning (SIIP), developed by NREL for constructing and solving unit commitment (UC) models using JuMP. SiennaNEM.jl provides tools to perform detailed studies of the NEM, leveraging PISP data to represent system parameters. It also includes post-processing, analysis, and visualization capabilities to deliver high-quality insights into system operations. Furthermore, SiennaNEM.jl benefits from Sienna's expanding tools, allowing users to extend the provided base studies with other Sienna features. Its foundation in JuMP also allows easy extraction and customization of the optimization model, making it highly adaptable for diverse studies.

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
using Revise
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
