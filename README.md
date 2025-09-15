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
cp -r NEM-reliability-suite/data/nem12 data/nem12
```

If you already clone it, pull to update to the latest data

```sh
cd NEM-reliability-suite
git pull
cd ..
cp -r NEM-reliability-suite/data/nem12 data/nem12
```

<!-- 
```sh
rm -rf data/nem12
cp -r NEM-reliability-suite/data/nem12-updated-csv data/nem12
```
-->

### Installation

```julia
using Pkg
Pkg.activate(".")
Pkg.instantiate()
Pkg.add("Revise")
using Revise
```

### Test

> [!NOTE]  
> Tests require the `data/nem12` directory, which is currently not released.

```julia
using Pkg

Pkg.activate(".")
Pkg.instantiate()
Pkg.precompile()
Pkg.test()
```
