# SiennaNEM

[![Build Status](https://github.com/yasirroni/SiennaNEM.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/yasirroni/SiennaNEM.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/yasirroni/SiennaNEM.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/yasirroni/SiennaNEM.jl)

## Usage

See minimum working example workflow in `examples/`.

## Development

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
