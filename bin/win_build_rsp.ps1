# Generates build.rsp for Makefile.win from the generated DuckDB manifests.

param([string]$Root = ".")

$duckdb = Join-Path $Root "c_src\duckdb"
$sources = Join-Path $duckdb ".sources"
$includes = Join-Path $duckdb ".include_dirs"

if (-not (Test-Path $sources)) {
    Write-Error "$sources is missing. Run bin/regen_duckdb.sh v2.0.0 first."
    exit 1
}

$lines = @("/std:c++17", "/EHsc", "/O2", "/MT", "/DNDEBUG=1", "/Ic_src")

foreach ($include in Get-Content $includes) {
    $lines += "/I" + (Join-Path $duckdb $include)
}

foreach ($source in Get-Content $sources) {
    $lines += Join-Path $duckdb $source
}

foreach ($nif in @("nif", "config", "term", "term_to_value", "value_to_term")) {
    $lines += Join-Path $Root "c_src\$nif.cpp"
}

Set-Content -Path (Join-Path $Root "build.rsp") -Value $lines -Encoding ASCII
