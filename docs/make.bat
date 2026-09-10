@ECHO OFF

pushd %~dp0

REM Command file for Sphinx documentation

if "%SPHINXBUILD%" == "" (
	set SPHINXBUILD=uv run sphinx-build
)
set SOURCEDIR=source
set BUILDDIR=build
set SPHINXPROJ=aiocometd

if "%1" == "" goto help

%SPHINXBUILD% >NUL 2>NUL
if errorlevel 9009 (
	echo.
	echo.The 'uv' command was not found. Install uv or set the SPHINXBUILD
	echo.environment variable to a working Sphinx command.
	echo.
	echo.See https://docs.astral.sh/uv/getting-started/installation/.
	exit /b 1
)

%SPHINXBUILD% -M %1 %SOURCEDIR% %BUILDDIR% %SPHINXOPTS%
goto end

:help
%SPHINXBUILD% -M help %SOURCEDIR% %BUILDDIR% %SPHINXOPTS%

:end
popd
